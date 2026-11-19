import os
import signal
import sys

import zmq

XSUB_PORT = os.getenv("XSUB_PORT", "5557")  # lado dos publicadores
XPUB_PORT = os.getenv("XPUB_PORT", "5558")  # lado dos assinantes


def main():
    ctx = zmq.Context()

    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(f"tcp://*:{XSUB_PORT}")

    xpub = ctx.socket(zmq.XPUB)
    xpub.bind(f"tcp://*:{XPUB_PORT}")

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    print(f"[proxy] publicadores (XSUB) em :{XSUB_PORT} | assinantes (XPUB) em :{XPUB_PORT}", flush=True)

    try:
        zmq.proxy(xsub, xpub)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        xsub.close()
        xpub.close()
        ctx.term()


if __name__ == "__main__":
    main()
