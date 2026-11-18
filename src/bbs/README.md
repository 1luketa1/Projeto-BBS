# BBS - Partes 1 a 4 (login, canais, Pub/Sub, relógios, heartbeat e eleição)

Stack: Python (servidor, broker, proxy, referência), Java (bots), ZeroMQ, Protocol Buffers, SQLite, Docker Compose.

## Executar
    docker compose up --build --no-log-prefix                  # referência, 1 servidor, 3 bots
    docker compose --profile multi up --build --no-log-prefix  # + server2 (necessário para eleição/Berkeley)
    docker compose logs -f reference                           # rank, lista e heartbeats
    docker compose down -v                                     # parar e apagar os bancos

## Estrutura
    proto/bbs.proto     contrato das mensagens (fonte única para Python e Java)
    common/bbsutil.py   relógio lógico, relógio físico ajustável e formatação de logs
    reference/          serviço de referência: rank, lista de servidores, heartbeat, hora correta
    broker/             ROUTER/DEALER (REQ/REP) - pedidos dos bots
    proxy/              XSUB/XPUB (Pub/Sub) - publicações nos canais
    server/             servidor: REP + PUB + SUB + REQ (referência) + SQLite
      cluster.py          coordenador, eleição (Bully) e sincronização do relógio
      berkeley.py         cálculo da hora correta (algoritmo de Berkeley)
      peer_link.py        comunicação com os outros servidores (porta 5560) e tópico "servers"
    tests/              testes da lógica da Parte 4 (sem Docker): python3 -m unittest discover -s tests -v
    client/             bot Java (Maven, JeroMQ, protobuf)
    docker-compose.yml  orquestração
