# peer_link.py - Comunicação deste servidor com os OUTROS servidores (ZeroMQ + protobuf).

# É a camada de rede usada por cluster.py:
#   * REQ direto para outro servidor (porta PEER_PORT, endereço = nome do serviço no Docker):
#       - "eleição"  -> REP: OK
#       - "relógio"  -> REP: hora correta (só o coordenador responde OK)
#   * PUB no tópico "servers" (via proxy) para anunciar que foi eleito coordenador.

# Cada pedido usa um socket REQ novo, fechado em seguida: assim um servidor fora do ar
# nunca deixa um socket "travado" (basta o timeout).
import zmq

import bbs_pb2

TOPIC = b"servers"   # tópico do Pub/Sub em que o coordenador é anunciado


class PeerLink:
    def __init__(self, ctx, name, reference, pub, stamp, logical, physical, logger, port, timeout_ms):
        self.ctx = ctx
        self.name = name
        self.reference = reference   # ReferenceClient: de onde vem a lista de servidores e ranks
        self.pub = pub               # socket PUB já conectado ao proxy
        self.stamp = stamp           # função que carimba timestamp + relógio lógico no envio
        self.logical = logical
        self.physical = physical
        self.logger = logger
        self.port = port
        self.timeout_ms = timeout_ms

    # ---- usado pelo Cluster -------------------------------------------
    def list_servers(self):
        return self.reference.list_servers()

    def ask_election(self, target: str, my_rank: int) -> bool:
        """REQ: eleição -> REP: OK. True se o alvo respondeu OK."""
        env = bbs_pb2.Envelope()
        env.election_req.name = self.name
        env.election_req.rank = my_rank
        result = self._request(target, env)
        if result is None:
            return False
        rep = result[0]
        return rep.WhichOneof("body") == "election_rep" and rep.election_rep.status == bbs_pb2.OK

    def request_clock(self, target: str, my_clock_ms: int):
        """
        REQ: relógio -> REP: hora correta.
        Devolve None (sem resposta) ou (ok, hora correta, hora do envio, hora do recebimento).
        """
        env = bbs_pb2.Envelope()
        env.clock_req.name = self.name
        env.clock_req.clock_ms = my_clock_ms
        result = self._request(target, env)
        if result is None:
            return None
        rep, sent_ms, received_ms = result
        if rep.WhichOneof("body") != "clock_rep":
            return (False, 0, sent_ms, received_ms)
        return (rep.clock_rep.status == bbs_pb2.OK, rep.clock_rep.correct_time, sent_ms, received_ms)

    def announce(self, name: str, rank: int) -> None:
        """PUB: [servers] [nome do coordenador]."""
        env = bbs_pb2.Envelope()
        env.coordinator_msg.name = name
        env.coordinator_msg.rank = rank
        self.stamp(env)
        self.pub.send_multipart([TOPIC, env.SerializeToString()])
        self.logger.log("PUB", env)

    # ---- interno ------------------------------------------------------
    def _request(self, target: str, env):
        """
        Envia ao servidor "target" e espera a resposta (até timeout_ms).
        Devolve (resposta, hora do envio, hora do recebimento) ou None se não respondeu.
        """
        sock = self.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        sock.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        try:
            sock.connect(f"tcp://{target}:{self.port}")
            self.stamp(env)                      # hora + relógio lógico incrementado antes de enviar
            sent_ms = env.timestamp
            self.logger.log("SEND", env, f"  -> {target}")
            sock.send(env.SerializeToString())
            raw = sock.recv()
        except zmq.ZMQError:                     # timeout (zmq.Again) ou destino inalcançável
            self.logger.note(f"sem resposta de '{target}' em {self.timeout_ms} ms")
            return None
        finally:
            sock.close(linger=0)

        received_ms = self.physical.now_ms()
        rep = bbs_pb2.Envelope.FromString(raw)
        self.logical.observe(rep.logical_clock)  # regra 2 de Lamport
        self.logger.log("RECV", rep, f"  <- {target}  local_lc={self.logical.value}")
        return rep, sent_ms, received_ms
