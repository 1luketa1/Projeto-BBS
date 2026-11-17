"""
proxy.py - Proxy Pub/Sub (XSUB/XPUB), separado do broker REQ/REP.

    Servidores (PUB) ---> [XSUB :5557   PROXY   XPUB :5558] ---> Bots (SUB)

Por que um proxy?
  * Publicadores (servidores) e assinantes (bots) só precisam conhecer UM
    endereço fixo cada, sem saber quantos existem do outro lado.
  * Servidores entram e saem sem os bots perceberem.
  * O XPUB repassa as inscrições dos bots para o XSUB, que as repassa aos
    publicadores - assim o tópico só trafega se alguém estiver inscrito.

Assim como o broker, o proxy NÃO interpreta as mensagens: só repassa os frames.
"""
import os
import signal
import sys

import zmq

XSUB_PORT = os.getenv("XSUB_PORT", "5557")  # lado dos publicadores (servidores)
XPUB_PORT = os.getenv("XPUB_PORT", "5558")  # lado dos assinantes (bots)


def main():
    ctx = zmq.Context()

    xsub = ctx.socket(zmq.XSUB)   # recebe as publicações dos servidores
    xsub.bind(f"tcp://*:{XSUB_PORT}")

    xpub = ctx.socket(zmq.XPUB)   # entrega aos bots inscritos
    xpub.bind(f"tcp://*:{XPUB_PORT}")

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    print(f"[proxy] publicadores (XSUB) em :{XSUB_PORT} | assinantes (XPUB) em :{XPUB_PORT}", flush=True)

    try:
        zmq.proxy(xsub, xpub)   # loop infinito: mensagens xsub->xpub, inscrições xpub->xsub
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        xsub.close()
        xpub.close()
        ctx.term()


if __name__ == "__main__":
    main()
