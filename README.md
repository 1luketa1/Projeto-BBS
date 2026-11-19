# BBS - simulação de um BBS com bots

Stack: Python (servidor, broker, proxy, referência), Java (bots), ZeroMQ, Protocol Buffers,
SQLite, Docker Compose.

## Execução
    docker compose up --build --no-log-prefix   # referência, broker, proxy, 2 servidores, 3 bots
    docker compose logs -f server1              # acompanhar um serviço
    docker compose down -v                      # parar e apagar os bancos


## Estrutura
    proto/bbs.proto     contrato das mensagens (fonte única para python e java)
    common/bbsutil.py   relógio lógico, relógio físico ajustável e formatação de logs
    reference/          rank, lista de servidores, heartbeat
    broker/             router/dealer, pedidos dos bots
    proxy/              pub/sub, publicações e anúncios
    server/             servidor
      server.py           laço principal e tratamento dos pedidos
      cluster.py          coordenador, eleição e sincronização do relógio
      berkeley.py         cálculo da hora correta
      peer_link.py        comunicação com os outros servidores (porta 5560) e pub/sub interno
      replication.py      replicação dos canais entre servidores
      reference_client.py conversa com o serviço de referência
      db.py               persistência via SQLite
    client/             bot java (Maven, JeroMQ, protobuf)
    docker-compose.yml  orquestração

## Como adicionar servidores
Cada servidor precisa de nome, volume e banco próprios. No `docker-compose.yml`:

1. Copie o bloco do `server2` e troque `server2` por `server3` em **três** lugares:
   o nome do serviço, `SERVER_NAME` e o volume. O `SERVER_NAME` deve ser igual ao nome do
   serviço, pois os outros servidores o encontram por `tcp://<SERVER_NAME>:5560` (DNS do Docker).
2. Declare `server3_data` na seção `volumes:` no fim do arquivo.
3. Com o sistema rodando, suba só o novo servidor, sem parar os outros:

       docker compose up -d --build server3

Observações: não use `--scale` nem `deploy.replicas` para servidores (cada um exige nome e
volume únicos). Esta versão replica apenas os canais; logins e mensagens ficam no banco do
servidor que os processou.
