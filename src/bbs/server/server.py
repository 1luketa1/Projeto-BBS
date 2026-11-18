import os
import re
import signal
import sys
import time

import zmq
from google.protobuf.message import DecodeError

import bbs_pb2  # gerado a partir de proto/bbs.proto durante o build da imagem
from bbsutil import LogicalClock, MessageLogger, PhysicalClock
from berkeley import BerkeleyCoordinator
from cluster import Cluster
from db import Database
from peer_link import TOPIC, PeerLink
from reference_client import ReferenceClient

# ---- Configuração (vem de variáveis de ambiente definidas no compose) --
SERVER_NAME = os.getenv("SERVER_NAME", "server")
BROKER_ADDR = os.getenv("BROKER_ADDR", "tcp://broker:5556")            # REQ/REP
PUB_ADDR = os.getenv("PUB_ADDR", "tcp://proxy:5557")                   # Pub/Sub (XSUB do proxy)
SUB_ADDR = os.getenv("SUB_ADDR", "tcp://proxy:5558")                   # Pub/Sub (XPUB do proxy)
REFERENCE_ADDR = os.getenv("REFERENCE_ADDR", "tcp://reference:5559")   # serviço de referência
PEER_PORT = int(os.getenv("PEER_PORT", "5560"))                        # porta para os outros servidores
DB_PATH = os.getenv("DB_PATH", "/data/server.db")
HEARTBEAT_EVERY = int(os.getenv("HEARTBEAT_EVERY", "10"))              # mensagens de clientes entre heartbeats
CLOCK_SYNC_EVERY = int(os.getenv("CLOCK_SYNC_EVERY", "15"))            # mensagens de clientes entre sincronizações
PEER_TIMEOUT_MS = int(os.getenv("PEER_TIMEOUT_MS", "1000"))            # espera por resposta de outro servidor
ELECTION_WAIT_SECONDS = float(os.getenv("ELECTION_WAIT_SECONDS", "5")) # espera pelo anúncio após um OK
CLOCK_SKEW_MS = int(os.getenv("SERVER_CLOCK_SKEW_MS", "0"))            # desvio simulado do relógio físico

# Regra de nomes (usuário e canal): letras, números, "_" e "-", de 1 a 32 caracteres.
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
MAX_TEXT = 500  # tamanho máximo de uma mensagem publicada
RESERVED_CHANNELS = {TOPIC.decode()}  # "servers" é o tópico do anúncio do coordenador

# ---- Relógios e log deste processo ------------------------------------
logical = LogicalClock()
physical = PhysicalClock(CLOCK_SKEW_MS)
logger = MessageLogger(SERVER_NAME, physical.now_ms)


def stamp(env: "bbs_pb2.Envelope") -> None:
    """
    Carimba a mensagem NO MOMENTO DO ENVIO: hora (relógio físico sincronizado)
    e relógio lógico, incrementado antes de enviar (regra 1 de Lamport).
    """
    env.timestamp = physical.now_ms()
    env.logical_clock = logical.tick()


def send_reply(sock, env: "bbs_pb2.Envelope") -> None:
    stamp(env)
    logger.log("SEND", env)
    sock.send(env.SerializeToString())


def new_reply() -> "bbs_pb2.Envelope":
    """Envelope de resposta vazio; o carimbo é aplicado por stamp() na hora de enviar."""
    return bbs_pb2.Envelope()


# ---------------------------------------------------------------------
# Pedidos dos CLIENTES (bots). Todos os handlers recebem (db, pub, req) e
# devolvem o Envelope de resposta. Só o de publicação usa o socket "pub".
# OBS: usamos SetInParent() para garantir que o campo do oneof fique
# "marcado" mesmo quando todos os valores são os padrão (ex.: status=OK=0).
# ---------------------------------------------------------------------
def handle_login(db: Database, pub, req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    reply = new_reply()
    reply.login_rep.SetInParent()
    username = req.login_req.username

    if not NAME_RE.match(username):
        reply.login_rep.status = bbs_pb2.ERROR
        reply.login_rep.error = "nome inválido (use 1-32 caracteres: letras, números, _ ou -)"
        return reply

    # Relogin do mesmo nome é permitido (o bot pode ter reiniciado).
    db.register_login(username, req.timestamp)
    reply.login_rep.status = bbs_pb2.OK
    return reply


def handle_create_channel(db: Database, pub, req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    reply = new_reply()
    reply.create_channel_rep.SetInParent()
    channel = req.create_channel_req.channel
    username = req.create_channel_req.username

    if not NAME_RE.match(channel):
        reply.create_channel_rep.status = bbs_pb2.ERROR
        reply.create_channel_rep.error = "nome de canal inválido"
    elif channel.lower() in RESERVED_CHANNELS:
        # "servers" é usado pelos servidores para anunciar o coordenador.
        reply.create_channel_rep.status = bbs_pb2.ERROR
        reply.create_channel_rep.error = "nome de canal reservado"
    elif not db.create_channel(channel, username, req.timestamp):
        reply.create_channel_rep.status = bbs_pb2.ERROR
        reply.create_channel_rep.error = "canal já existe"
    else:
        reply.create_channel_rep.status = bbs_pb2.OK
    return reply


def handle_list_channels(db: Database, pub, req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    reply = new_reply()
    reply.list_channels_rep.SetInParent()
    reply.list_channels_rep.channels.extend(db.list_channels())
    return reply


def handle_publish(db: Database, pub, req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    """
    Valida o pedido, grava a mensagem no banco (persistência em disco), publica no tópico do canal
    e só então responde ao bot com o status (OK ou erro).
    """
    reply = new_reply()
    reply.publish_rep.SetInParent()
    p = req.publish_req

    def fail(msg: str) -> "bbs_pb2.Envelope":
        reply.publish_rep.status = bbs_pb2.ERROR
        reply.publish_rep.error = msg
        return reply

    if not NAME_RE.match(p.username):
        return fail("nome de usuário inválido")
    if not p.text.strip():
        return fail("mensagem vazia")
    if len(p.text) > MAX_TEXT:
        return fail(f"mensagem maior que {MAX_TEXT} caracteres")

    # Resolve o nome oficial do canal ("Geral" -> "geral"); None = não existe.
    channel = db.find_channel(p.channel)
    if channel is None:
        return fail("canal não existe")

    # Mensagem que os assinantes vão receber.
    published = new_reply()
    published.channel_msg.SetInParent()
    published.channel_msg.channel = channel
    published.channel_msg.username = p.username
    published.channel_msg.text = p.text
    stamp(published)  # hora e relógio lógico do envio da publicação

    db.add_message(channel, p.username, p.text, req.timestamp, published.timestamp,
                   SERVER_NAME, req.logical_clock, published.logical_clock)

    # 2 frames: [tópico = nome do canal] [Envelope serializado]
    pub.send_multipart([channel.encode("utf-8"), published.SerializeToString()])
    logger.log("PUB", published)

    reply.publish_rep.status = bbs_pb2.OK
    return reply


# Tabela de despacho: nome do campo do oneof -> função que trata.
HANDLERS = {
    "login_req": handle_login,
    "create_channel_req": handle_create_channel,
    "list_channels_req": handle_list_channels,
    "publish_req": handle_publish,
}


def process(db: Database, pub, raw: bytes) -> "bbs_pb2.Envelope":
    """Transforma os bytes recebidos de um cliente na resposta (sempre devolve algo)."""
    try:
        req = bbs_pb2.Envelope.FromString(raw)
    except DecodeError:
        reply = new_reply()
        reply.error_rep.error = "mensagem inválida (não é um Envelope protobuf)"
        print(f"[{SERVER_NAME}] RECV bytes inválidos ({len(raw)} bytes)", flush=True)
        return reply

    logical.observe(req.logical_clock)  # regra 2 de Lamport: máximo entre o recebido e o atual
    logger.log("RECV", req, f"  local_lc={logical.value}")

    handler = HANDLERS.get(req.WhichOneof("body"))
    if handler is None:
        reply = new_reply()
        reply.error_rep.error = "tipo de mensagem não suportado"
        return reply
    return handler(db, pub, req)


# ---------------------------------------------------------------------
# Pedidos de OUTROS SERVIDORES (socket peer) e anúncios do coordenador (SUB)
# ---------------------------------------------------------------------
def process_peer(cluster: Cluster, raw: bytes) -> "bbs_pb2.Envelope":
    """Trata "relógio" (só o coordenador responde OK) e "eleição" (OK para rank menor)."""
    reply = new_reply()
    try:
        req = bbs_pb2.Envelope.FromString(raw)
    except DecodeError:
        reply.error_rep.error = "mensagem inválida"
        return reply

    logical.observe(req.logical_clock)
    logger.log("RECV", req, f"  local_lc={logical.value}")

    kind = req.WhichOneof("body")
    if kind == "clock_req":
        reply.clock_rep.SetInParent()
        correct = cluster.handle_clock_request(req.clock_req.name, req.clock_req.clock_ms)
        if correct is None:
            reply.clock_rep.status = bbs_pb2.ERROR
            reply.clock_rep.error = "este servidor não é o coordenador"
        else:
            reply.clock_rep.status = bbs_pb2.OK
            reply.clock_rep.correct_time = correct       # a "hora correta"
    elif kind == "election_req":
        reply.election_rep.SetInParent()
        if cluster.on_election_request(req.election_req.name, req.election_req.rank):
            reply.election_rep.status = bbs_pb2.OK       # "OK": assumo a eleição depois de responder
        else:
            reply.election_rep.status = bbs_pb2.ERROR
            reply.election_rep.error = "meu rank não é maior que o seu"
    else:
        reply.error_rep.error = "tipo de mensagem não suportado"
    return reply


def process_announcement(cluster: Cluster, topic: bytes, payload: bytes) -> None:
    """Anúncio de coordenador recebido no tópico "servers"."""
    if topic != TOPIC:   # o filtro do ZeroMQ casa por prefixo: confere o tópico exato
        return
    try:
        env = bbs_pb2.Envelope.FromString(payload)
    except DecodeError:
        return
    if env.WhichOneof("body") != "coordinator_msg":
        return
    if env.coordinator_msg.name == SERVER_NAME:
        return           # eco do meu próprio anúncio

    logical.observe(env.logical_clock)
    logger.log("RECV", env, f"  local_lc={logical.value}")
    cluster.on_announcement(env.coordinator_msg.name, env.coordinator_msg.rank)


def main() -> None:
    db = Database(DB_PATH)
    ctx = zmq.Context()

    # 1) Antes de atender clientes: descobre o rank e vê quem mais está disponível.
    reference = ReferenceClient(ctx, REFERENCE_ADDR, SERVER_NAME, logical, physical, logger)
    rank = reference.register()
    reference.list_servers()

    # 2) Sockets: broker (clientes), proxy (publicar e assinar) e porta para os outros servidores.
    rep = ctx.socket(zmq.REP)
    rep.connect(BROKER_ADDR)       # o servidor se conecta ao broker (não o contrário)
    pub = ctx.socket(zmq.PUB)
    pub.connect(PUB_ADDR)
    sub = ctx.socket(zmq.SUB)
    sub.connect(SUB_ADDR)
    sub.subscribe(TOPIC)           # recebe o anúncio de coordenador
    peer = ctx.socket(zmq.REP)
    peer.bind(f"tcp://*:{PEER_PORT}")

    link = PeerLink(ctx, SERVER_NAME, reference, pub, stamp, logical, physical, logger,
                    PEER_PORT, PEER_TIMEOUT_MS)
    cluster = Cluster(SERVER_NAME, rank, physical, BerkeleyCoordinator(), link, logger,
                      election_wait_s=ELECTION_WAIT_SECONDS)

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    logger.note(f"pronto | rank={rank} | broker={BROKER_ADDR} | peer=:{PEER_PORT} | db={DB_PATH} "
                f"| desvio do relógio simulado={CLOCK_SKEW_MS} ms")

    # 3) Dá tempo para as inscrições chegarem ao proxy (senão o 1º anúncio pode se perder)
    #    e faz a eleição inicial: ainda não há coordenador conhecido.
    time.sleep(1)
    cluster.start_election()

    poller = zmq.Poller()
    for sock in (rep, peer, sub):
        poller.register(sock, zmq.POLLIN)

    received = 0   # mensagens de clientes recebidas (base do heartbeat e da sincronização)
    try:
        while True:
            events = dict(poller.poll(1000))   # espera até 1 s por qualquer um dos três sockets

            if rep in events:                  # pedido de um bot
                reply = process(db, pub, rep.recv())
                send_reply(rep, reply)         # responde ao cliente primeiro...
                received += 1
                if received % HEARTBEAT_EVERY == 0:
                    reference.heartbeat()      # ...depois avisa a referência (a cada 10)
                if received % CLOCK_SYNC_EVERY == 0:
                    cluster.sync_clock()       # ...e sincroniza o relógio com o coordenador (a cada 15)

            if peer in events:                 # pedido de outro servidor (eleição ou relógio)
                send_reply(peer, process_peer(cluster, peer.recv()))
                cluster.run_pending_election() # depois do "OK", assume a eleição

            if sub in events:                  # anúncio de coordenador
                topic, payload = sub.recv_multipart()
                process_announcement(cluster, topic, payload)

            cluster.maintain()                 # sem coordenador conhecido? tenta eleger
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        for sock in (rep, pub, sub, peer):
            sock.close()
        ctx.term()


if __name__ == "__main__":
    main()
