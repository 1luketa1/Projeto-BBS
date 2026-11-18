"""
reference.py - Serviço de referência (Parte 3).

Conversa SOMENTE com os servidores (socket REP na porta 5559); não recebe
mensagens dos clientes. Responsabilidades:
  1. informar o rank do servidor        REQ: nome -> REP: rank
  2. armazenar a lista de servidores    nome + rank, sem repetição de nomes
  3. fornecer a lista de servidores     REQ: list -> REP: [nomes e ranks]
  4. atualizar a lista pelo heartbeat   quem ficar HEARTBEAT_TIMEOUT_SECONDS
                                        sem enviar heartbeat é removido
  (A hora correta deixou de vir daqui na Parte 4: o relógio dos servidores é
   sincronizado pelo coordenador eleito, com o algoritmo de Berkeley.)

As mensagens também carregam timestamp e relógio lógico (a referência tem o seu contador).
"""
import os
import re
import signal
import sys
import time

import zmq
from google.protobuf.message import DecodeError

import bbs_pb2  # gerado a partir de proto/bbs.proto durante o build da imagem
from bbsutil import LogicalClock, MessageLogger
from registry import Registry

PORT = os.getenv("REFERENCE_PORT", "5559")
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "30"))

NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

# A referência é a fonte da hora correta: usa o relógio do sistema sem ajuste.
logical = LogicalClock()
registry = Registry(HEARTBEAT_TIMEOUT)


def now_ms() -> int:
    return int(time.time() * 1000)


logger = MessageLogger("reference", now_ms)


def stamp(env: "bbs_pb2.Envelope") -> None:
    """Carimba no momento do envio (hora + relógio lógico incrementado antes de enviar)."""
    env.timestamp = now_ms()
    env.logical_clock = logical.tick()


# ---------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------
def handle_rank(req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    reply = bbs_pb2.Envelope()
    reply.rank_rep.SetInParent()
    name = req.rank_req.name
    if not NAME_RE.match(name):
        reply.rank_rep.status = bbs_pb2.ERROR
        reply.rank_rep.error = "nome de servidor inválido"
    else:
        reply.rank_rep.status = bbs_pb2.OK
        reply.rank_rep.rank = registry.register(name)   # nome repetido devolve o mesmo rank
    return reply


def handle_list(req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    reply = bbs_pb2.Envelope()
    reply.list_servers_rep.SetInParent()
    for name, rank in registry.listing():
        info = reply.list_servers_rep.servers.add()
        info.name = name
        info.rank = rank
    return reply


def handle_heartbeat(req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    reply = bbs_pb2.Envelope()
    reply.heartbeat_rep.SetInParent()
    if registry.heartbeat(req.heartbeat_req.name):
        reply.heartbeat_rep.status = bbs_pb2.OK
    else:
        reply.heartbeat_rep.status = bbs_pb2.ERROR
        reply.heartbeat_rep.error = "servidor não cadastrado (faça um novo pedido de rank)"
    return reply


HANDLERS = {
    "rank_req": handle_rank,
    "list_servers_req": handle_list,
    "heartbeat_req": handle_heartbeat,
}


def process(raw: bytes) -> "bbs_pb2.Envelope":
    try:
        req = bbs_pb2.Envelope.FromString(raw)
    except DecodeError:
        reply = bbs_pb2.Envelope()
        reply.error_rep.error = "mensagem inválida"
        return reply

    logical.observe(req.logical_clock)   # regra 2: máximo entre o recebido e o atual
    logger.log("RECV", req, f"  local_lc={logical.value}")

    handler = HANDLERS.get(req.WhichOneof("body"))
    if handler is None:
        reply = bbs_pb2.Envelope()
        reply.error_rep.error = "tipo de mensagem não suportado"
        return reply
    return handler(req)


def main() -> None:
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.bind(f"tcp://*:{PORT}")

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    logger.note(f"pronto na porta {PORT} | heartbeat expira em {HEARTBEAT_TIMEOUT:.0f} s")

    try:
        while True:
            # Espera até 1 s por uma mensagem; assim a limpeza roda mesmo sem tráfego.
            events = dict(poller.poll(1000))

            for name in registry.purge():
                logger.note(f"servidor '{name}' removido da lista (sem heartbeat há mais de "
                            f"{HEARTBEAT_TIMEOUT:.0f} s) | disponíveis: {registry.listing()}")

            if sock in events:
                reply = process(sock.recv())
                stamp(reply)
                logger.log("SEND", reply)
                sock.send(reply.SerializeToString())
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        sock.close()
        ctx.term()


if __name__ == "__main__":
    main()
