# BBS - simulação de um BBS com bots (Partes 1 a 4)

Stack: Python (servidor, broker, proxy, referência), Java (bots), ZeroMQ, Protocol Buffers,
SQLite, Docker Compose.

## O que cada parte entrega
| Parte | Funcionalidade |
|---|---|
| 1 | Login de bots, criação e listagem de canais (REQ/REP); mensagens em Protocol Buffers; persistência em SQLite |
| 2 | Publicação em canais por Pub/Sub (proxy XSUB/XPUB); bots se inscrevem em canais; mensagens persistidas |
| 3 | Relógio lógico (Lamport) em todas as mensagens; serviço de referência (rank, lista de servidores, heartbeat) |
| 4 | Eleição de coordenador (Bully) e sincronização do relógio físico (Berkeley) a cada 15 mensagens; anúncio no tópico `servers` |
| extra | Replicação dos canais entre servidores (para qualquer servidor conhecer todos os canais) |

## Executar
    docker compose up --build --no-log-prefix   # referência, broker, proxy, 2 servidores, 3 bots
    docker compose logs -f server1              # acompanhar um serviço
    docker compose down -v                       # parar e apagar os bancos

Testes da lógica (sem Docker):

    python3 -m unittest discover -s tests -v

## Estrutura
    proto/bbs.proto     contrato das mensagens (fonte única para Python e Java)
    common/bbsutil.py   relógio lógico, relógio físico ajustável e formatação de logs
    reference/          serviço de referência: rank, lista de servidores, heartbeat
    broker/             ROUTER/DEALER (REQ/REP) - pedidos dos bots
    proxy/              XSUB/XPUB (Pub/Sub) - publicações e anúncios
    server/             servidor
      server.py           laço principal (Poller) e tratamento dos pedidos
      cluster.py          coordenador, eleição (Bully) e sincronização do relógio
      berkeley.py         cálculo da hora correta (algoritmo de Berkeley)
      peer_link.py        comunicação com os outros servidores (porta 5560) e Pub/Sub interno
      replication.py      replicação dos canais entre servidores
      reference_client.py conversa com o serviço de referência
      db.py               persistência SQLite
    client/             bot Java (Maven, JeroMQ, protobuf)
    tests/              testes da Parte 4 e da replicação
    docker-compose.yml  orquestração

## Como adicionar servidores
Cada servidor precisa de nome, volume e banco próprios. No `docker-compose.yml`:

1. Copie o bloco do `server2` e troque `server2` por `server3` em **três** lugares:
   o nome do serviço, `SERVER_NAME` e o volume. O `SERVER_NAME` deve ser igual ao nome do
   serviço, pois os outros servidores o encontram por `tcp://<SERVER_NAME>:5560` (DNS do Docker).
2. Declare `server3_data` na seção `volumes:` no fim do arquivo.
3. Com o sistema rodando, suba só o novo servidor, sem parar os outros:

       docker compose up -d --build server3

O que acontece sozinho: o servidor pede o rank à referência (recebe o próximo número), faz a
eleição (maior rank vira coordenador), busca os canais dos outros servidores e o broker passa
a distribuir pedidos para ele. Para remover: `docker compose stop server3`. Depois de 30 s sem
heartbeat a referência o tira da lista; se ele era o coordenador, os outros percebem na próxima
sincronização de relógio (15 mensagens) e elegem outro.

Observações: não use `--scale` nem `deploy.replicas` para servidores (cada um exige nome e
volume únicos). Esta versão replica apenas os canais; logins e mensagens ficam no banco do
servidor que os processou.
