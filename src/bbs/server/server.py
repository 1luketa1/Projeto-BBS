import os
import re
import signal
import sys

import zmq
from google.protobuf.message import DecodeError

import bbs_pb2  # gerado a partir de proto/bbs.proto durante o build da imagem
from bbsutil import LogicalClock, MessageLogger, PhysicalClock
from db import Database
from reference_client import ReferenceClient

# ---- config --
SERVER_NAME = os.getenv("SERVER_NAME", "server")
BROKER_ADDR = os.getenv("BROKER_ADDR", "tcp://broker:5556")            
PUB_ADDR = os.getenv("PUB_ADDR", "tcp://proxy:5557")                    
REFERENCE_ADDR = os.getenv("REFERENCE_ADDR", "tcp://reference:5559")    
DB_PATH = os.getenv("DB_PATH", "/data/server.db")
HEARTBEAT_EVERY = int(os.getenv("HEARTBEAT_EVERY", "10"))              
CLOCK_SKEW_MS = int(os.getenv("SERVER_CLOCK_SKEW_MS", "0"))            

# Regra de nomes (usuário e canal): letras, números, "_" e "-", de 1 a 32 caracteres.
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
MAX_TEXT = 500  # tamanho máximo de uma mensagem publicada

# ---- relógicos e log ---
logical = LogicalClock()
physical = PhysicalClock(CLOCK_SKEW_MS)
logger = MessageLogger(SERVER_NAME, physical.now_ms)


def stamp(env: "bbs_pb2.Envelope") -> None:
    env.timestamp = physical.now_ms()
    env.logical_clock = logical.tick()


def send_reply(sock, env: "bbs_pb2.Envelope") -> None:
    stamp(env)
    logger.log("SEND", env)
    sock.send(env.SerializeToString())


def new_reply() -> "bbs_pb2.Envelope":
    return bbs_pb2.Envelope()


# --- handlers ---

def handle_login(db: Database, pub, req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    reply = new_reply()
    reply.login_rep.SetInParent()
    username = req.login_req.username

    if not NAME_RE.match(username):
        reply.login_rep.status = bbs_pb2.ERROR
        reply.login_rep.error = "nome inválido (use 1-32 caracteres: letras, números, _ ou -)"
        return reply

    # relogin do mesmo nome é permitido
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

    # resolve o nome oficial do canal
    channel = db.find_channel(p.channel)
    if channel is None:
        return fail("canal não existe")

    # mensagem que os assinantes vão receber
    published = new_reply()
    published.channel_msg.SetInParent()
    published.channel_msg.channel = channel
    published.channel_msg.username = p.username
    published.channel_msg.text = p.text
    stamp(published)  # hora e relógio lógico do envio da publicação

    db.add_message(channel, p.username, p.text, req.timestamp, published.timestamp,
                   SERVER_NAME, req.logical_clock, published.logical_clock)

    pub.send_multipart([channel.encode("utf-8"), published.SerializeToString()])
    logger.log("PUB", published)

    reply.publish_rep.status = bbs_pb2.OK
    return reply


HANDLERS = {
    "login_req": handle_login,
    "create_channel_req": handle_create_channel,
    "list_channels_req": handle_list_channels,
    "publish_req": handle_publish,
}


def process(db: Database, pub, raw: bytes) -> "bbs_pb2.Envelope":
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


def main() -> None:
    db = Database(DB_PATH)
    ctx = zmq.Context()

    reference = ReferenceClient(ctx, REFERENCE_ADDR, SERVER_NAME, logical, physical, logger)
    rank = reference.register()
    reference.list_servers()

    rep = ctx.socket(zmq.REP)
    rep.connect(BROKER_ADDR)
    pub = ctx.socket(zmq.PUB)
    pub.connect(PUB_ADDR)

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    logger.note(f"pronto | rank={rank} | broker={BROKER_ADDR} | proxy={PUB_ADDR} | db={DB_PATH} "
                f"| desvio do relógio simulado={CLOCK_SKEW_MS} ms")

    received = 0
    try:
        while True:
            raw = rep.recv()
            reply = process(db, pub, raw)
            send_reply(rep, reply)

            received += 1
            if received % HEARTBEAT_EVERY == 0:
                reference.heartbeat()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        rep.close()
        pub.close()
        ctx.term()


if __name__ == "__main__":
    main()
