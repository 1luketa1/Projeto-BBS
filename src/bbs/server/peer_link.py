import time

import zmq

import bbs_pb2

TOPIC = b"servers"


class PeerLink:
    def __init__(self, ctx, name, reference, pub, stamp, logical, physical, logger, port, timeout_ms):
        self.ctx = ctx
        self.name = name
        self.reference = reference
        self.pub = pub               
        self.stamp = stamp
        self.logical = logical
        self.physical = physical
        self.logger = logger
        self.port = port
        self.timeout_ms = timeout_ms
        
        self.peer_sock = None
        self.serve_peer = None

    # --- usado pelo cluster ---
    def list_servers(self, quiet: bool = False):
        return self.reference.list_servers(log_result=not quiet)

    def ask_election(self, target: str, my_rank: int) -> bool:
        env = bbs_pb2.Envelope()
        env.election_req.name = self.name
        env.election_req.rank = my_rank
        result = self._request(target, env)
        if result is None:
            return False
        rep = result[0]
        return rep.WhichOneof("body") == "election_rep" and rep.election_rep.status == bbs_pb2.OK

    def request_clock(self, target: str, my_clock_ms: int):
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

    def sync_channels(self, target: str):
        env = bbs_pb2.Envelope()
        env.sync_channels_req.name = self.name
        result = self._request(target, env)
        if result is None:
            return None
        rep = result[0]
        if rep.WhichOneof("body") != "sync_channels_rep":
            return None
        return [(c.name, c.created_by, c.created_at) for c in rep.sync_channels_rep.channels]

    def replicate_channel(self, target: str, name: str, created_by: str, created_at: int) -> bool:
        env = bbs_pb2.Envelope()
        env.channel_replicated.origin = self.name
        env.channel_replicated.name = name
        env.channel_replicated.created_by = created_by
        env.channel_replicated.created_at = created_at
        result = self._request(target, env)
        if result is None:
            return False
        rep = result[0]
        return rep.WhichOneof("body") == "replicate_channel_rep" and rep.replicate_channel_rep.status == bbs_pb2.OK

    def announce(self, name: str, rank: int) -> None:
        env = bbs_pb2.Envelope()
        env.coordinator_msg.name = name
        env.coordinator_msg.rank = rank
        self.stamp(env)
        self.pub.send_multipart([TOPIC, env.SerializeToString()])
        self.logger.log("PUB", env)

    # --- interno ---
    def _wait_reply(self, sock):
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        if self.peer_sock is not None:
            poller.register(self.peer_sock, zmq.POLLIN)

        deadline = time.monotonic() + self.timeout_ms / 1000
        while True:
            remaining_ms = int((deadline - time.monotonic()) * 1000)
            if remaining_ms <= 0:
                return None
            events = dict(poller.poll(remaining_ms))
            if self.peer_sock is not None and self.peer_sock in events:
                self.serve_peer()
            if sock in events:
                return sock.recv()

    def _request(self, target: str, env):
        sock = self.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        try:
            sock.connect(f"tcp://{target}:{self.port}")
            self.stamp(env)
            sent_ms = env.timestamp
            self.logger.log("SEND", env, f"  -> {target}")
            sock.send(env.SerializeToString())
            raw = self._wait_reply(sock)
        except zmq.ZMQError:
            raw = None
        finally:
            sock.close(linger=0)

        if raw is None:
            self.logger.note(f"sem resposta de '{target}' em {self.timeout_ms} ms")
            return None

        received_ms = self.physical.now_ms()
        rep = bbs_pb2.Envelope.FromString(raw)
        self.logical.observe(rep.logical_clock)
        self.logger.log("RECV", rep, f"  <- {target}  local_lc={self.logical.value}")
        return rep, sent_ms, received_ms
