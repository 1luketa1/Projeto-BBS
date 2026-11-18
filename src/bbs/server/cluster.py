"""
cluster.py - Coordenador, eleição (algoritmo do Valentão / Bully) e sincronização de relógio.

Lógica pura: não usa ZeroMQ nem protobuf. Toda comunicação com os outros servidores
passa pelo objeto "link" (peer_link.py), que oferece:
    list_servers()                       -> [(nome, rank), ...] ou None
    ask_election(alvo, meu_rank)         -> True se o alvo respondeu OK
    announce(nome, rank)                 -> publica no tópico "servers"
    request_clock(alvo, minha_hora_ms)   -> None (sem resposta) ou (ok, hora_correta, enviado_ms, recebido_ms)
Isso também permite testar a lógica com um "link" falso (ver tests/).

Bully (resumo):
  * o servidor de MAIOR rank vivo é o coordenador;
  * quem quer eleição pede (REQ: eleição) aos servidores de rank maior que o seu;
  * se algum responde OK, ele assume a eleição (repete o processo acima dele) e, no fim,
    o de maior rank anuncia no tópico "servers";
  * se ninguém de rank maior responde, este servidor se declara coordenador e anuncia.
"""
import time


class Cluster:
    def __init__(self, name, rank, physical, berkeley, link, logger,
                 election_wait_s: float = 5.0, monotonic=time.monotonic) -> None:
        self.name = name
        self.rank = rank
        self.physical = physical     # relógio físico ajustável (bbsutil.PhysicalClock)
        self.berkeley = berkeley     # cálculo da hora correta (usado quando este servidor é o coordenador)
        self.link = link
        self.logger = logger
        self.election_wait_s = election_wait_s   # quanto esperar o anúncio depois de um OK
        self.monotonic = monotonic               # injetável para testes

        # Variável exigida pela especificação: nome do servidor eleito coordenador.
        # None = ainda desconhecido (sem eleição concluída).
        self.coordinator = None

        self.pending_election = False   # recebemos um pedido de eleição e devemos assumi-la
        self.retry_at = 0.0             # quando repetir a eleição se nenhum anúncio chegar

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
        # Só servidores de rank MAIOR participam; os mais altos primeiro (mais prováveis de serem o coordenador).
        higher = sorted(((rank, name) for name, rank in servers if rank > self.rank), reverse=True)
        self.logger.note(f"eleição iniciada (meu rank={self.rank}) | servidores de rank maior: "
                         f"{[name for _, name in higher] or 'nenhum'}")

        for rank, name in higher:
            if self.link.ask_election(name, self.rank):
                self.logger.note(f"'{name}' (rank {rank}) respondeu OK e assume a eleição; aguardando o anúncio")
                self.retry_at = self.monotonic() + self.election_wait_s
                return

        self.become_coordinator()   # ninguém de rank maior respondeu

    def become_coordinator(self) -> None:
        self.berkeley.reset()
        self.coordinator = self.name
        self.link.announce(self.name, self.rank)
        self.logger.note(f"fui eleito coordenador (rank {self.rank})")

    def on_election_request(self, name: str, rank: int) -> bool:
        """
        Recebido (REQ: eleição). Responde OK se quem pediu tem rank MENOR que o meu;
        depois da resposta, o servidor deve assumir a eleição (ver run_pending_election).
        """
        if rank < self.rank:
            self.pending_election = True
            return True
        return False

    def run_pending_election(self) -> None:
        """Chamar depois de enviar o "OK": agora este servidor assume a eleição."""
        if self.pending_election:
            self.pending_election = False
            self.start_election()

    def on_announcement(self, name: str, rank: int) -> None:
        """Recebido no tópico "servers": alguém se declarou coordenador."""
        if name == self.name:
            return   # eco do meu próprio anúncio
        if rank < self.rank:
            # Um servidor de rank menor se declarou coordenador, mas eu tenho rank maior: contesto.
            self.logger.note(f"'{name}' (rank {rank}) se anunciou coordenador, mas meu rank ({self.rank}) "
                             f"é maior: assumindo a eleição")
            self.start_election()
            return
        if self.is_coordinator():
            self.berkeley.reset()   # deixei de ser o coordenador
        if self.coordinator != name:
            self.logger.note(f"novo coordenador: '{name}' (rank {rank})")
        self.coordinator = name

    def maintain(self) -> None:
        """Chamar a cada volta do laço principal: sem coordenador conhecido, tenta eleger."""
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
        """
        Coordenador: recebe (REQ: relógio) e devolve a hora correta (média de Berkeley).
        Devolve None se este servidor não é o coordenador. O próprio coordenador também se ajusta à média.
        """
        if not self.is_coordinator():
            return None
        now = self.physical.now_ms()
        average, used = self.berkeley.compute(name, reported_ms, now)
        self.physical.offset_ms += average - now
        self.logger.note(f"Berkeley: '{name}' está {reported_ms - now:+d} ms em relação a mim | "
                         f"média de {used} relógio(s) | meu ajuste: {average - now:+d} ms")
        return average
