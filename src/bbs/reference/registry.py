import time


class Registry:
    def __init__(self, timeout_seconds: float, clock=time.monotonic) -> None:
        self.timeout = timeout_seconds
        self.clock = clock              # injetável para facilitar testes
        self.servers = {}               # nome -> {"rank": int, "last_seen": float}
        self.next_rank = 1

    def register(self, name: str) -> int:
        #cadastra o server e devolve o rank, nomes repetidos não criam outra entrada
        if name not in self.servers:
            self.servers[name] = {"rank": self.next_rank, "last_seen": self.clock()}
            self.next_rank += 1
        else:
            self.servers[name]["last_seen"] = self.clock()
        return self.servers[name]["rank"]

    def heartbeat(self, name: str) -> bool:
        if name not in self.servers:
            return False
        self.servers[name]["last_seen"] = self.clock()
        return True

    def purge(self) -> list:
        now = self.clock()
        expired = [n for n, s in self.servers.items() if now - s["last_seen"] > self.timeout]
        for name in expired:
            del self.servers[name]
        return expired

    def listing(self) -> list:
        """[(nome, rank), ...] dos servidores disponíveis, ordenado por rank."""
        return sorted(((n, s["rank"]) for n, s in self.servers.items()), key=lambda item: item[1])
