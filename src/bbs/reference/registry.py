"""
registry.py - Lista de servidores cadastrados no serviço de referência.

Fica separado do reference.py (que usa ZeroMQ e protobuf) para conter só a
regra de negócio: ranks, nomes sem repetição e expiração por falta de heartbeat.
"""
import time


class Registry:
    def __init__(self, timeout_seconds: float, clock=time.monotonic) -> None:
        self.timeout = timeout_seconds
        self.clock = clock              # injetável para facilitar testes
        self.servers = {}               # nome -> {"rank": int, "last_seen": float}
        self.next_rank = 1

    def register(self, name: str) -> int:
        """
        Cadastra o servidor e devolve o rank. Nome repetido NÃO cria outra
        entrada: devolve o rank que o servidor já tinha (e renova a presença).
        """
        if name not in self.servers:
            self.servers[name] = {"rank": self.next_rank, "last_seen": self.clock()}
            self.next_rank += 1
        else:
            self.servers[name]["last_seen"] = self.clock()
        return self.servers[name]["rank"]

    def heartbeat(self, name: str) -> bool:
        """Renova a presença. False se o servidor não está cadastrado (ex.: já foi removido)."""
        if name not in self.servers:
            return False
        self.servers[name]["last_seen"] = self.clock()
        return True

    def purge(self) -> list:
        """Remove quem ficou tempo demais sem heartbeat. Devolve os nomes removidos."""
        now = self.clock()
        expired = [n for n, s in self.servers.items() if now - s["last_seen"] > self.timeout]
        for name in expired:
            del self.servers[name]
        return expired

    def listing(self) -> list:
        """[(nome, rank), ...] dos servidores disponíveis, ordenado por rank."""
        return sorted(((n, s["rank"]) for n, s in self.servers.items()), key=lambda item: item[1])
