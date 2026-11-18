"""
reference_client.py - Lado do servidor na conversa com o serviço de referência.

Usa um socket REQ ligado diretamente à referência (não passa pelo broker).
Operações (diagramas da Parte 3):
  register()      REQ: nome  -> REP: rank
  list_servers()  REQ: list  -> REP: [lista com nomes e ranks]
  heartbeat()     REQ: nome  -> REP: OK

Na Parte 4 o relógio deixou de ser sincronizado pelo heartbeat: quem fornece a hora
correta é o coordenador eleito entre os servidores (ver cluster.py e peer_link.py).

Todas as mensagens carregam timestamp e relógio lógico, como as demais.
"""
import time

import zmq

import bbs_pb2

TIMEOUT_MS = 2000   # espera máxima por uma resposta da referência


class ReferenceClient:
    def __init__(self, ctx, addr, name, logical, physical, logger):
        self.ctx = ctx
        self.addr = addr
        self.name = name
        self.logical = logical     # relógio lógico do servidor (compartilhado)
        self.physical = physical   # relógio físico do servidor (compartilhado)
        self.logger = logger
        self.rank = None
        self.sock = None
        self._connect()

    def _connect(self) -> None:
        """(Re)cria o socket REQ. Necessário após timeout: um REQ sem resposta fica "travado"."""
        if self.sock is not None:
            self.sock.close(linger=0)
        self.sock = self.ctx.socket(zmq.REQ)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
        self.sock.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
        self.sock.connect(self.addr)

    def _request(self, env):
        """
        Carimba (timestamp + relógio lógico), envia e espera a resposta.
        Devolve a resposta ou None em caso de timeout.
        """
        env.timestamp = self.physical.now_ms()
        env.logical_clock = self.logical.tick()          # regra 1: incrementa ANTES de enviar
        self.logger.log("SEND", env)
        try:
            self.sock.send(env.SerializeToString())
            raw = self.sock.recv()
        except zmq.Again:
            self._connect()
            return None

        rep = bbs_pb2.Envelope.FromString(raw)
        self.logical.observe(rep.logical_clock)          # regra 2: máximo entre o recebido e o atual
        self.logger.log("RECV", rep, f"  local_lc={self.logical.value}")
        return rep

    # ------------------------------------------------------------------
    def register(self) -> int:
        """Pede o rank. Repete até a referência responder (o servidor depende do rank para operar)."""
        while True:
            env = bbs_pb2.Envelope()
            env.rank_req.name = self.name
            rep = self._request(env)
            if rep is not None and rep.WhichOneof("body") == "rank_rep" \
                    and rep.rank_rep.status == bbs_pb2.OK:
                self.rank = rep.rank_rep.rank
                return self.rank
            self.logger.note("serviço de referência indisponível, nova tentativa em 1 s...")
            time.sleep(1)

    def list_servers(self):
        """
        Pede a lista de servidores disponíveis, exibe e devolve [(nome, rank), ...].
        Devolve None se a referência não respondeu.
        """
        env = bbs_pb2.Envelope()
        env.list_servers_req.SetInParent()
        rep = self._request(env)
        if rep is None or rep.WhichOneof("body") != "list_servers_rep":
            self.logger.note("não foi possível obter a lista de servidores")
            return None
        servers = [(s.name, s.rank) for s in rep.list_servers_rep.servers]
        self.logger.note("servidores disponíveis: " + ", ".join(f"{n}(rank {r})" for n, r in servers))
        return servers

    def heartbeat(self) -> None:
        """Avisa que está vivo. Se a referência não nos conhece mais, cadastra de novo."""
        env = bbs_pb2.Envelope()
        env.heartbeat_req.name = self.name
        rep = self._request(env)
        if rep is None:
            self.logger.note("heartbeat sem resposta da referência")
            return
        if rep.WhichOneof("body") != "heartbeat_rep" or rep.heartbeat_rep.status != bbs_pb2.OK:
            # A referência removeu este servidor por falta de heartbeat: cadastra de novo.
            self.logger.note("referência não reconhece este servidor, registrando de novo")
            self.register()
