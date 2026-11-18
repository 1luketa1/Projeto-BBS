import time

import zmq

import bbs_pb2

TIMEOUT_MS = 2000   # espera máxima por uma resposta da referência


class ReferenceClient:
    def __init__(self, ctx, addr, name, logical, physical, logger):
        self.ctx = ctx
        self.addr = addr
        self.name = name
        self.logical = logical     # relógio lógico do servidor
        self.physical = physical   # relógio físico do servidor
        self.logger = logger
        self.rank = None
        self.sock = None
        self._connect()

    def _connect(self) -> None:
        #recria o socket REQ, que é necessário após timeout
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
        sent_ms = env.timestamp
        self.logger.log("SEND", env)
        try:
            self.sock.send(env.SerializeToString())
            raw = self.sock.recv()
        except zmq.Again:
            self._connect()
            return None
        received_ms = self.physical.now_ms()

        rep = bbs_pb2.Envelope.FromString(raw)
        self.logical.observe(rep.logical_clock)
        self.logger.log("RECV", rep, f"  local_lc={self.logical.value}")
        return rep, sent_ms, received_ms

    def register(self) -> int:
        while True:
            env = bbs_pb2.Envelope()
            env.rank_req.name = self.name
            result = self._request(env)
            if result is not None:
                rep = result[0]
                if rep.WhichOneof("body") == "rank_rep" and rep.rank_rep.status == bbs_pb2.OK:
                    self.rank = rep.rank_rep.rank
                    return self.rank
            self.logger.note("serviço de referência indisponível, nova tentativa em 1 s...")
            time.sleep(1)

    def list_servers(self) -> None:
        env = bbs_pb2.Envelope()
        env.list_servers_req.SetInParent()
        result = self._request(env)
        if result is None or result[0].WhichOneof("body") != "list_servers_rep":
            self.logger.note("não foi possível obter a lista de servidores")
            return
        servers = ", ".join(f"{s.name}(rank {s.rank})" for s in result[0].list_servers_rep.servers)
        self.logger.note(f"servidores disponíveis: {servers}")

    def heartbeat(self) -> None:
        env = bbs_pb2.Envelope()
        env.heartbeat_req.name = self.name
        result = self._request(env)
        if result is None:
            self.logger.note("heartbeat sem resposta da referência")
            return

        rep, sent_ms, received_ms = result
        hb = rep.heartbeat_rep
        if rep.WhichOneof("body") != "heartbeat_rep" or hb.status != bbs_pb2.OK:
            self.logger.note("referência não reconhece este servidor, registrando de novo")
            self.register()
            return

        adjustment, rtt = self.physical.sync(hb.reference_time, sent_ms, received_ms)
        self.logger.note(f"relógio físico sincronizado: ajuste={adjustment:+d} ms (rtt={rtt} ms)")
