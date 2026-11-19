import time


class Cluster:
    def __init__(self, name, rank, physical, berkeley, link, logger,
                 election_wait_s: float = 5.0, monotonic=time.monotonic) -> None:
        self.name = name
        self.rank = rank
        self.physical = physical
        self.berkeley = berkeley
        self.link = link
        self.logger = logger
        self.election_wait_s = election_wait_s
        self.monotonic = monotonic

        self.coordinator = None

        self.pending_election = False
        self.retry_at = 0.0          

    def is_coordinator(self) -> bool:
        return self.coordinator == self.name

    # ------------------------------------------------------------------
    # Eleição
    # ------------------------------------------------------------------
    def start_election(self) -> None:
        servers = self.link.list_servers()
        if servers is None:
            self.logger.note("eleição adiada: lista de servidores indisponível")
            self.retry_at = self.monotonic() + self.election_wait_s
            return

        self.coordinator = None
        higher = sorted(((rank, name) for name, rank in servers if rank > self.rank), reverse=True)
        self.logger.note(f"eleição iniciada (meu rank={self.rank}) | servidores de rank maior: "
                         f"{[name for _, name in higher] or 'nenhum'}")

        for rank, name in higher:
            if self.link.ask_election(name, self.rank):
                self.logger.note(f"'{name}' (rank {rank}) respondeu OK e assume a eleição; aguardando o anúncio")
                self.retry_at = self.monotonic() + self.election_wait_s
                return

        self.become_coordinator()

    def become_coordinator(self) -> None:
        self.berkeley.reset()
        self.coordinator = self.name
        self.link.announce(self.name, self.rank)
        self.logger.note(f"fui eleito coordenador (rank {self.rank})")

    def on_election_request(self, name: str, rank: int) -> bool:
        if rank < self.rank:
            self.pending_election = True
            return True
        return False

    def run_pending_election(self) -> None:
        if self.pending_election:
            self.pending_election = False
            self.start_election()

    def on_announcement(self, name: str, rank: int) -> None:
        if name == self.name:
            return
        if rank < self.rank:
            self.logger.note(f"'{name}' (rank {rank}) se anunciou coordenador, mas meu rank ({self.rank}) "
                             f"é maior: assumindo a eleição")
            self.start_election()
            return
        if self.is_coordinator():
            self.berkeley.reset()
        if self.coordinator != name:
            self.logger.note(f"novo coordenador: '{name}' (rank {rank})")
        self.coordinator = name

    def maintain(self) -> None:
        if self.coordinator is None and self.monotonic() >= self.retry_at:
            self.start_election()

    # ------------------------------------------------------------------
    # Sincronização do relógio (Berkeley)
    # ------------------------------------------------------------------
    def sync_clock(self) -> None:
        """Chamada a cada 15 mensagens: pede a hora correta ao coordenador (REQ: relógio -> REP: hora correta)."""
        if self.coordinator is None:
            return   # maintain() cuida de eleger
        if self.is_coordinator():
            self.logger.note("sou o coordenador: meu relógio se ajusta quando os outros servidores pedem a hora")
            return

        result = self.link.request_clock(self.coordinator, self.physical.now_ms())
        if result is None:
            self.logger.note(f"coordenador '{self.coordinator}' indisponível: iniciando eleição")
            self.start_election()
            return

        ok, correct_ms, sent_ms, received_ms = result
        if not ok:
            self.logger.note(f"'{self.coordinator}' não se considera coordenador: iniciando eleição")
            self.start_election()
            return

        adjustment, rtt = self.physical.sync(correct_ms, sent_ms, received_ms)
        self.logger.note(f"relógio sincronizado com o coordenador '{self.coordinator}': "
                         f"ajuste={adjustment:+d} ms (rtt={rtt} ms)")

    def handle_clock_request(self, name: str, reported_ms: int):
        if not self.is_coordinator():
            return None
        now = self.physical.now_ms()
        average, used = self.berkeley.compute(name, reported_ms, now)
        self.physical.offset_ms += average - now
        self.logger.note(f"Berkeley: '{name}' está {reported_ms - now:+d} ms em relação a mim | "
                         f"média de {used} relógio(s) | meu ajuste: {average - now:+d} ms")
        return average
