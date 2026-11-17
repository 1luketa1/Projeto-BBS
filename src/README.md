# BBS - Partes 1 e 2 (login, canais e publicação Pub/Sub)

Stack: Python (servidor, broker e proxy), Java (bots), ZeroMQ, Protocol Buffers, SQLite, Docker Compose.

## Executar
    docker compose up --build --no-log-prefix   # 1 servidor, 3 bots
    docker compose --profile multi up --build   # 2 servidores (sem replicação ainda)
    docker compose logs -f server1              # acompanhar um serviço
    docker compose down -v                      # parar e apagar os bancos

## Estrutura
    proto/bbs.proto     contrato das mensagens (fonte única para Python e Java)
    broker/             ROUTER/DEALER (REQ/REP) - pedidos dos bots
    proxy/              XSUB/XPUB (Pub/Sub) - publicações nos canais
    server/             servidor: REP (pedidos) + PUB (canais) + SQLite
    client/             bot Java (Maven, JeroMQ, protobuf)
    docker-compose.yml  orquestração
