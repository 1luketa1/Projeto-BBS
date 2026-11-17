import os
import re
import signal
import sys
import time
import zmq
from google.protobuf import text_format
from google.protobuf.message import DecodeError

import bbs_pb2  # gerado a partir de proto/bbs.proto durante o build da imagem
from db import Database

# --- config  --
SERVER_NAME = os.getenv("SERVER_NAME", "server")
BROKER_ADDR = os.getenv("BROKER_ADDR", "tcp://broker:5556")
DB_PATH = os.getenv("DB_PATH", "/data/server.db")

# regra de nomes (usuário e canal): letras, números, "_" e "-", de 1 a 32 caracteres
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


# ---------------------------------------------------------------------
# utilidades
# ---------------------------------------------------------------------
def now_ms() -> int:
    # deixa em milissegundos, talvez mude
    return int(time.time() * 1000)


def log(direction: str, env: "bbs_pb2.Envelope") -> None:
    #exibição das mensagens
    kind = env.WhichOneof("body") or "(vazio)"
    clock = time.strftime("%H:%M:%S") + f".{now_ms() % 1000:03d}"
    content = text_format.MessageToString(env, as_one_line=True)
    print(f"[{clock}] [{SERVER_NAME}] {direction} {kind:<20} | {content}", flush=True)


def new_reply() -> "bbs_pb2.Envelope":
    """Cria um Envelope de resposta já com o timestamp do envio."""
    return bbs_pb2.Envelope(timestamp=now_ms())


def handle_login(db: Database, req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    reply = new_reply()
    reply.login_rep.SetInParent()
    username = req.login_req.username

    if not NAME_RE.match(username):
        reply.login_rep.status = bbs_pb2.ERROR
        reply.login_rep.error = "nome inválido (use 1-32 caracteres: letras, números, _ ou -)"
        return reply

    # re-login do mesmo nome é permitido
    db.register_login(username, req.timestamp)
    reply.login_rep.status = bbs_pb2.OK
    return reply


def handle_create_channel(db: Database, req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
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


def handle_list_channels(db: Database, req: "bbs_pb2.Envelope") -> "bbs_pb2.Envelope":
    reply = new_reply()
    reply.list_channels_rep.SetInParent()
    reply.list_channels_rep.channels.extend(db.list_channels())
    return reply


HANDLERS = {
    "login_req": handle_login,
    "create_channel_req": handle_create_channel,
    "list_channels_req": handle_list_channels,
}


def process(db: Database, raw: bytes) -> "bbs_pb2.Envelope":
    """Transforma os bytes recebidos na resposta (sempre devolve algo)."""
    try:
        req = bbs_pb2.Envelope.FromString(raw)
    except DecodeError:
        reply = new_reply()
        reply.error_rep.error = "mensagem inválida (não é um Envelope protobuf)"
        print(f"[{SERVER_NAME}] RECV bytes inválidos ({len(raw)} bytes)", flush=True)
        return reply

    log("RECV", req)

    handler = HANDLERS.get(req.WhichOneof("body"))
    if handler is None:
        reply = new_reply()
        reply.error_rep.error = "tipo de mensagem não suportado"
        return reply
    return handler(db, req)


def main() -> None:
    db = Database(DB_PATH)

    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.connect(BROKER_ADDR)  # conexão do server com o broker

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    print(f"[{SERVER_NAME}] pronto | broker={BROKER_ADDR} | db={DB_PATH}", flush=True)

    try:
        while True:
            raw = sock.recv()           # bloqueia até chegar uma requisição
            reply = process(db, raw)
            log("SEND", reply)
            sock.send(reply.SerializeToString())
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        sock.close()
        ctx.term()


if __name__ == "__main__":
    main()
