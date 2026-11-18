"""
berkeley.py - Cálculo da "hora correta" pelo coordenador (algoritmo de Berkeley).

Berkeley, em resumo: o coordenador reúne as horas de todos os relógios, calcula a
MÉDIA (ignorando relógios muito distantes) e cada processo se ajusta para essa média.
Nenhum relógio é tratado como "o certo": o grupo converge para a média.

Adaptação ao protocolo da especificação (quem pergunta é o servidor:
REQ: relógio -> REP: hora correta):
  * o pedido "relógio" já traz a hora atual de quem pergunta; o coordenador guarda essa amostra;
  * ao responder, calcula a média da sua própria hora com as amostras mais recentes dos
    outros servidores (cada amostra é "avançada" até o instante atual, pois os relógios
    continuam andando entre uma pergunta e outra);
  * a média é a "hora correta": vai para quem perguntou e também é adotada pelo próprio coordenador.

Lógica pura (sem ZeroMQ/protobuf), para poder ser testada sem rede.
"""


class BerkeleyCoordinator:
    def __init__(self, max_diff_ms: int = 60_000, max_age_ms: int = 60_000) -> None:
        self.max_diff_ms = max_diff_ms   # relógio mais distante que isto do coordenador é ignorado (outlier)
        self.max_age_ms = max_age_ms     # amostra mais velha que isto é esquecida (servidor silencioso)
        # nome do servidor -> (hora dele, hora do coordenador no instante em que foi registrada)
        self.samples = {}

    def reset(self) -> None:
        """Esquece tudo (chamado quando este servidor assume ou perde a coordenação)."""
        self.samples.clear()

    def compute(self, name: str, reported_ms: int, my_now_ms: int):
        """
        Registra a hora informada por "name" e devolve (hora correta, quantidade de relógios na média).
        """
        self.samples[name] = (reported_ms, my_now_ms)

        clocks = [my_now_ms]   # o relógio do próprio coordenador sempre entra na média
        for other, (clock, at) in list(self.samples.items()):
            age = my_now_ms - at
            if age > self.max_age_ms:
                del self.samples[other]
                continue
            projected = clock + age                              # "avança" a amostra até agora
            if abs(projected - my_now_ms) <= self.max_diff_ms:   # ignora relógios absurdos
                clocks.append(projected)

        average = sum(clocks) // len(clocks)
        # Quem perguntou vai adotar a média: guarda isso como a nova hora dele.
        self.samples[name] = (average, my_now_ms)
        return average, len(clocks)
