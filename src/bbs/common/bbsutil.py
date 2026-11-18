import time
from datetime import datetime


# ---------------------------------------------------------------------
# Relógio lógico
# ---------------------------------------------------------------------
class LogicalClock:

    def __init__(self) -> None:
        self.value = 0  # inicia junto com o processo

    def tick(self) -> int:
        self.value += 1
        return self.value

    def observe(self, received: int) -> int:
        self.value = max(self.value, received)
        return self.value


# ---------------------------------------------------------------------
# Relógio físico ajustável
# ---------------------------------------------------------------------
class PhysicalClock:
    """
    Hora do sistema + um deslocamento (offset) que a sincronização vai ajustando.
    "skew_ms" simula um relógio desajustado desde o início (útil para demonstrar
    a sincronização, já que todos os containers compartilham o mesmo relógio real).
    """

    def __init__(self, skew_ms: int = 0) -> None:
        self.offset_ms = skew_ms

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self.offset_ms

    def sync(self, reference_ms: int, sent_ms: int, received_ms: int):
        """
        Ajuste no estilo de Cristian: a hora da referência já está "velha" pelo
        tempo de viagem da resposta, estimado em metade do RTT.
          sent_ms / received_ms = leituras deste relógio ao enviar o pedido / receber a resposta.
        Devolve (ajuste aplicado em ms, rtt em ms).
        """
        rtt = received_ms - sent_ms
        reference_now = reference_ms + rtt // 2
        adjustment = reference_now - received_ms
        self.offset_ms += adjustment
        return adjustment, rtt


# ---------------------------------------------------------------------
# Log das mensagens
# ---------------------------------------------------------------------
def fmt_clock(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%H:%M:%S.%f")[:-3]


def fmt_value(field, value) -> str:
    if field.type == field.TYPE_ENUM:
        return field.enum_type.values_by_number[value].name
    if field.type == field.TYPE_MESSAGE:
        return "{" + describe_message(value) + "}"
    return str(value)


def describe_message(msg) -> str:
    parts = []
    for f in msg.DESCRIPTOR.fields:
        value = getattr(msg, f.name)
        if isinstance(value, (str, bytes, int, float, bool)):
            if value == "":
                continue
            parts.append(f"{f.name}={fmt_value(f, value)}")
        else:
            parts.append(f"{f.name}=[{', '.join(fmt_value(f, v) for v in value)}]")
    return " ".join(parts)


class MessageLogger:

    def __init__(self, name: str, now_ms) -> None:
        self.name = name
        self.now_ms = now_ms

    def log(self, direction: str, env, extra: str = "") -> None:
        kind = env.WhichOneof("body") or "(vazio)"
        body = describe_message(getattr(env, kind)) if kind != "(vazio)" else ""
        content = (body + "  " if body else "") + f"ts={fmt_clock(env.timestamp)}  lc={env.logical_clock}{extra}"
        print(f"{fmt_clock(self.now_ms())} | {self.name:<10} | {direction:<4} | {kind:<19} | {content}",
              flush=True)

    def note(self, text: str) -> None:
        print(f"{fmt_clock(self.now_ms())} | {self.name:<10} | INFO | {text}", flush=True)
