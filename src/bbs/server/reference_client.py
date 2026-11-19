import time

import zmq

import bbs_pb2

TIMEOUT_MS = 2000   # espera máxima por uma resposta da referência


class ReferenceClient:
    def __init__(self, ctx, addr, name, logical, physical, logger):
        self.ctx = ctx
        self.addr = addr
        self.name = name
        self.logical = logical
        self.physical = physical
        self.logger = logger
        self.rank = None
        self.sock = None
        self._connect()

    def _connect(self) -> None:
        if self.sock is not None:
            self.sock.close(linger=0)
        self.sock = self.ctx.socket(zmq.REQ)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
        self.sock.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
        self.sock.connect(self.addr)

    def _request(self, env):
        env.timestamp = self.physical.now_ms()
        env.logical_clock = self.logical.tick()
        self.logger.log("SEND", env)
        try:
            self.sock.send(env.SerializeToString())
            raw = self.sock.recv()
        except zmq.Again:
            self._connect()
            return None

        rep = bbs_pb2.Envelope.FromString(raw)
        self.logical.observe(rep.logical_clock)
        self.logger.log("RECV", rep, f"  local_lc={self.logical.value}")
        return rep
    
    def register(self) -> int:
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

    def list_servers(self, log_result: bool = True):
        env = bbs_pb2.Envelope()
        env.list_servers_req.SetInParent()
        rep = self._request(env)
        if rep is None or rep.WhichOneof("body") != "list_servers_rep":
            self.logger.note("não foi possível obter a lista de servidores")
            return None
        servers = [(s.name, s.rank) for s in rep.list_servers_rep.servers]
        if log_result:
            self.logger.note("servidores disponíveis: " + ", ".join(f"{n}(rank {r})" for n, r in servers))
        return servers

    def heartbeat(self) -> None:
        env = bbs_pb2.Envelope()
        env.heartbeat_req.name = self.name
        rep = self._request(env)
        if rep is None:
            self.logger.note("heartbeat sem resposta da referência")
            return
        if rep.WhichOneof("body") != "heartbeat_rep" or rep.heartbeat_rep.status != bbs_pb2.OK:
            self.logger.note("referência não reconhece este servidor, registrando de novo")
            self.register()
