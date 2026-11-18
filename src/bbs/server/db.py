import os
import sqlite3


class Database:
    def __init__(self, path: str):
        # Garante que a pasta do arquivo exista
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(path)
        self._create_tables()

    def _create_tables(self):
        # "with self.conn" faz commit automático (ou rollback se der erro)
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
            self._add_column_if_missing("messages", "sent_clock", "INTEGER NOT NULL DEFAULT 0")
            self._add_column_if_missing("messages", "published_clock", "INTEGER NOT NULL DEFAULT 0")

    def _add_column_if_missing(self, table: str, column: str, ddl: str) -> None:
        columns = [row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")]
        if column not in columns:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    # --- logins ---
    def register_login(self, username: str, timestamp: int) -> None:
        #Guarda o login (mesmo usuário pode aparecer várias vezes)
        with self.conn:
            self.conn.execute(
                "INSERT INTO logins (username, timestamp) VALUES (?, ?)",
                (username, timestamp),
            )

    # --- canais ---
    def create_channel(self, name: str, created_by: str, timestamp: int) -> bool:
        #cria o canal, retorna false se já existir um com esse nome
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO channels (name, created_by, timestamp) VALUES (?, ?, ?)",
                    (name, created_by, timestamp),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def list_channels(self) -> list[str]:
        #nomes de todos os canais existentes em ordem de criação
        rows = self.conn.execute("SELECT name FROM channels ORDER BY id").fetchall()
        return [r[0] for r in rows]

    def find_channel(self, name: str):
        
        # devolve o nome "oficial" do canal ou None se não existir
        # não é case sensitive
        # o servidor publica sempre com o nome oficial como tópico
        
        row = self.conn.execute("SELECT name FROM channels WHERE name = ?", (name,)).fetchone()
        return row[0] if row else None

    # ---- mensagens ----------------------------------------------------
    def add_message(self, channel: str, username: str, text: str,
                    sent_at: int, published_at: int, server: str,
                    sent_clock: int, published_clock: int) -> None:
        
        # Guarda uma mensagem publicada com todos os dados da troca: quem, onde,
        # o quê, quando o bot enviou, quando o servidor publicou, qual servidor
        # tratou e os dois valores de relógio lógico (foi util!)
        
        with self.conn:
            self.conn.execute(
                "INSERT INTO messages (channel, username, text, sent_at, published_at, server, "
                "sent_clock, published_clock) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (channel, username, text, sent_at, published_at, server, sent_clock, published_clock),
            )
