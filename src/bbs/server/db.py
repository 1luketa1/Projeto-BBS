"""
db.py - Camada de persistência (SQLite).

Cada servidor tem o SEU PRÓPRIO arquivo .db (a especificação proíbe
compartilhar o arquivo entre servidores). O arquivo fica em um volume
Docker, então sobrevive a reinicializações do container.

Tabelas:
  logins   -> todo login realizado, com o timestamp          (Parte 1)
  channels -> todos os canais criados (nome único)           (Parte 1)
  messages -> toda mensagem publicada em um canal            (Parte 2)
              + relógios lógicos do envio e da publicação    (Parte 3)
"""
import os
import sqlite3


class Database:
    def __init__(self, path: str):
        # Garante que a pasta do arquivo exista (ex.: /data).
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(path)
        self._create_tables()

    def _create_tables(self):
        # "with self.conn" faz commit automático (ou rollback se der erro).
        with self.conn:
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS logins (
                       id        INTEGER PRIMARY KEY AUTOINCREMENT,
                       username  TEXT    NOT NULL,
                       timestamp INTEGER NOT NULL   -- epoch em ms
                   )"""
            )
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS channels (
                       id         INTEGER PRIMARY KEY AUTOINCREMENT,
                       -- UNIQUE + NOCASE: "Geral" e "geral" contam como duplicados
                       name       TEXT    NOT NULL UNIQUE COLLATE NOCASE,
                       created_by TEXT    NOT NULL,
                       timestamp  INTEGER NOT NULL
                   )"""
            )
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS messages (
                       id              INTEGER PRIMARY KEY AUTOINCREMENT,
                       channel         TEXT    NOT NULL,   -- canal (tópico) em que foi publicada
                       username        TEXT    NOT NULL,   -- autor
                       text            TEXT    NOT NULL,   -- conteúdo da mensagem
                       sent_at         INTEGER NOT NULL,   -- timestamp do pedido enviado pelo bot (ms)
                       published_at    INTEGER NOT NULL,   -- timestamp da publicação feita pelo servidor (ms)
                       server          TEXT    NOT NULL,   -- qual servidor processou/publicou
                       sent_clock      INTEGER NOT NULL DEFAULT 0,  -- relógio lógico enviado pelo bot
                       published_clock INTEGER NOT NULL DEFAULT 0   -- relógio lógico da publicação
                   )"""
            )
            # Bancos criados na Parte 2 não têm as colunas de relógio lógico:
            # acrescenta sem perder os dados já gravados.
            self._add_column_if_missing("messages", "sent_clock", "INTEGER NOT NULL DEFAULT 0")
            self._add_column_if_missing("messages", "published_clock", "INTEGER NOT NULL DEFAULT 0")

    def _add_column_if_missing(self, table: str, column: str, ddl: str) -> None:
        columns = [row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")]
        if column not in columns:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    # ---- logins -------------------------------------------------------
    def register_login(self, username: str, timestamp: int) -> None:
        """Guarda o login (mesmo usuário pode aparecer várias vezes)."""
        with self.conn:
            self.conn.execute(
                "INSERT INTO logins (username, timestamp) VALUES (?, ?)",
                (username, timestamp),
            )

    # ---- canais -------------------------------------------------------
    def create_channel(self, name: str, created_by: str, timestamp: int) -> bool:
        """Cria o canal. Retorna False se já existir um com esse nome."""
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO channels (name, created_by, timestamp) VALUES (?, ?, ?)",
                    (name, created_by, timestamp),
                )
            return True
        except sqlite3.IntegrityError:  # violou o UNIQUE
            return False

    def list_channels(self) -> list[str]:
        """Nomes de todos os canais existentes, em ordem de criação."""
        rows = self.conn.execute("SELECT name FROM channels ORDER BY id").fetchall()
        return [r[0] for r in rows]

    def find_channel(self, name: str):
        """
        Devolve o nome "oficial" do canal (como foi criado) ou None se não existir.
        A busca ignora maiúsculas/minúsculas, então "Geral" encontra "geral".
        O servidor publica sempre com o nome oficial como tópico.
        """
        row = self.conn.execute("SELECT name FROM channels WHERE name = ?", (name,)).fetchone()
        return row[0] if row else None

    # ---- mensagens ----------------------------------------------------
    def add_message(self, channel: str, username: str, text: str,
                    sent_at: int, published_at: int, server: str,
                    sent_clock: int, published_clock: int) -> None:
        """
        Guarda uma mensagem publicada com TODOS os dados da troca: quem, onde,
        o quê, quando o bot enviou, quando o servidor publicou, qual servidor
        tratou e os dois valores de relógio lógico (permitem reconstruir a
        ordem causal das mensagens depois).
        """
        with self.conn:
            self.conn.execute(
                "INSERT INTO messages (channel, username, text, sent_at, published_at, server, "
                "sent_clock, published_clock) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (channel, username, text, sent_at, published_at, server, sent_clock, published_clock),
            )
