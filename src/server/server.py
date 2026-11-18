import os
import re
import signal
import sys
import time
from datetime import datetime

import zmq
from google.protobuf.message import DecodeError

import bbs_pb2  # gerado a partir de proto/bbs.proto durante o build da imagem
from db import Database

# ---- config ---
SERVER_NAME = os.getenv("SERVER_NAME", "server")
BROKER_ADDR = os.getenv("BROKER_ADDR", "tcp://broker:5556")
PUB_ADDR = os.getenv("PUB_ADDR", "tcp://proxy:5557")
DB_PATH = os.getenv("DB_PATH", "/data/server.db")

# regra de nomes (usuário e canal): letras, números, "_" e "-", de 1 a 32 caracteres
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
MAX_TEXT = 500  # tamanho máximo de uma mensagem publicada


# ---------------------------------------------------------------------
# utilidaddes
# ---------------------------------------------------------------------
def now_ms() -> int:
    return int(time.time() * 1000)


def fmt_value(field, value) -> str:
    #valor legível de um campo (enum vira o nome, ex.: 0 -> OK)
    if field.type == field.TYPE_ENUM:
        return field.enum_type.values_by_number[value].name
    return str(value)


def describe_body(env: "bbs_pb2.Envelope") -> str:
    """
    Lista TODOS os campos da mensagem como chave=valor, inclusive os que
    têm valor padrão (ex.: status=OK, que o formato texto do protobuf omitiria).
    Strings vazias (ex.: error quando não há erro) são omitidas para não poluir.
    Funciona para qualquer mensagem nova que for adicionada ao .proto.
    """
    kind = env.WhichOneof("body")
    if kind is None:
        return ""                   #ignora mensagens em branco
    body = getattr(env, kind)
    parts = []
    for f in body.DESCRIPTOR.fields:
        value = getattr(body, f.name)
        if isinstance(value, (str, bytes, int, float, bool)):
            if value == "":
                continue
            parts.append(f"{f.name}={fmt_value(f, value)}")
        else:
            parts.append(f"{f.name}=[{', '.join(fmt_value(f, v) for v in value)}]")
    return " ".join(parts)


def log(direction: str, env: "bbs_pb2.Envelope") -> None:
    #exibição das mensagens
    kind = env.WhichOneof("body") or "(vazio)"
    clock = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    sent_at = datetime.fromtimestamp(env.timestamp / 1000).strftime("%H:%M:%S.%f")[:-3]
    body = describe_body(env)
    content = f"{body}  ts={sent_at}" if body else f"ts={sent_at}"
    print(f"{clock} | {SERVER_NAME:<10} | {direction:<4} | {kind:<19} | {content}", flush=True)


def new_reply() -> "bbs_pb2.Envelope":
    """Cria um Envelope de resposta já com o timestamp do envio."""
    return bbs_pb2.Envelope(timestamp=now_ms())


# ---------------------------------------------------------------------
# Handlers: um por tipo de requisição. Todos recebem (db, pub, req) e
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

    # Mensagem que os assinantes vão receber (com o timestamp do envio da publicação).
    published = new_reply()
    published.channel_msg.SetInParent()
    published.channel_msg.channel = channel
    published.channel_msg.username = p.username
    published.channel_msg.text = p.text

    db.add_message(channel, p.username, p.text, req.timestamp, published.timestamp, SERVER_NAME)

    # 2 frames: [tópico = nome do canal] [Envelope serializado]
    pub.send_multipart([channel.encode("utf-8"), published.SerializeToString()])
    log("PUB", published)

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
    return handler(db, pub, req)


def main() -> None:
    db = Database(DB_PATH)

    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    rep.connect(BROKER_ADDR)   # o servidor se conecta ao broker (não o contrário)

    pub = ctx.socket(zmq.PUB)
    pub.connect(PUB_ADDR)      # e ao proxy Pub/Sub (XSUB)

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    print(f"[{SERVER_NAME}] pronto | broker={BROKER_ADDR} | proxy={PUB_ADDR} | db={DB_PATH}", flush=True)

    try:
        while True:
            raw = rep.recv()            # bloqueia até chegar uma requisição
            reply = process(db, pub, raw)
            log("SEND", reply)
            rep.send(reply.SerializeToString())
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        rep.close()
        pub.close()
        ctx.term()


if __name__ == "__main__":
    main()
