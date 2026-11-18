# BBS - Partes 1 a 3 (login, canais, Pub/Sub, relógios e heartbeat)

Stack: Python (servidor, broker, proxy, referência), Java (bots), ZeroMQ, Protocol Buffers, SQLite, Docker Compose.

## Executar
    docker compose up --build --no-log-prefix                  # referência, 1 servidor, 3 bots
    docker compose --profile multi up --build --no-log-prefix  # + server2 (bancos sem replicação ainda)
    docker compose logs -f reference                           # rank, lista e heartbeats
    docker compose down -v                                     # parar e apagar os bancos

## Estrutura
    proto/bbs.proto     contrato das mensagens (fonte única para Python e Java)
    common/bbsutil.py   relógio lógico, relógio físico ajustável e formatação de logs
    reference/          serviço de referência: rank, lista de servidores, heartbeat, hora correta
    broker/             ROUTER/DEALER (REQ/REP) - pedidos dos bots
    proxy/              XSUB/XPUB (Pub/Sub) - publicações nos canais
    server/             servidor: REP + PUB + REQ (referência) + SQLite
    client/             bot Java (Maven, JeroMQ, protobuf)
    docker-compose.yml  orquestração
