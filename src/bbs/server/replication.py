import time


class Replicator:
    def __init__(self, db, name, link, logger, monotonic=time.monotonic, skip_seconds: float = 10.0) -> None:
        self.db = db
        self.name = name
        self.link = link
        self.logger = logger
        self.monotonic = monotonic
        self.skip_seconds = skip_seconds
        self.skip_until = {}

    def _skipped(self, server: str) -> bool:
        return self.monotonic() < self.skip_until.get(server, 0.0)

    def _mark_unresponsive(self, server: str) -> None:
        self.skip_until[server] = self.monotonic() + self.skip_seconds
        self.logger.note(f"replicação: '{server}' não respondeu; não insistirei por {self.skip_seconds:.0f} s")

    def apply(self, name: str, created_by: str, created_at: int) -> bool:
        """Grava o canal se ainda não existir. True se era novo."""
        return self.db.create_channel(name, created_by, created_at)

    def on_peer_request(self, origin: str, name: str, created_by: str, created_at: int) -> None:
        """Outro servidor pediu para replicar um canal criado nele."""
        if self.apply(name, created_by, created_at):
            self.logger.note(f"replicação: canal '{name}' recebido de '{origin}'")

    def replicate_to_peers(self, name: str, created_by: str, created_at: int) -> int:
        servers = self.link.list_servers(quiet=True)
        if servers is None:
            self.logger.note("replicação adiada: lista de servidores indisponível (a conferência corrige depois)")
            return 0
        confirmed = 0
        for server, _rank in servers:
            if server == self.name or self._skipped(server):
                continue
            if self.link.replicate_channel(server, name, created_by, created_at):
                confirmed += 1
            else:
                self._mark_unresponsive(server)
        return confirmed

    def pull_from_peers(self) -> int:
        servers = self.link.list_servers(quiet=True)
        if servers is None:
            return 0
        added = 0
        for server, _rank in servers:
            if server == self.name or self._skipped(server):
                continue
            rows = self.link.sync_channels(server)
            if rows is None:
                self._mark_unresponsive(server)
                continue
            for name, created_by, created_at in rows:
                if self.apply(name, created_by, created_at):
                    added += 1
        if added:
            self.logger.note(f"replicação: {added} canal(is) novo(s) obtido(s) dos outros servidores")
        return added
