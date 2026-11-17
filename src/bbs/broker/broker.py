
import os
import signal
import sys

import zmq

FRONTEND_PORT = os.getenv("FRONTEND_PORT", "5555")  # lado dos clientes
BACKEND_PORT = os.getenv("BACKEND_PORT", "5556")    # lado dos servidores


def main():
    ctx = zmq.Context()

    frontend = ctx.socket(zmq.ROUTER)   # fala com os REQ dos clientes
    frontend.bind(f"tcp://*:{FRONTEND_PORT}")

    backend = ctx.socket(zmq.DEALER)    # fala com os REP dos servidores
    backend.bind(f"tcp://*:{BACKEND_PORT}")

    # Permite que "docker stop" (SIGTERM) encerre o processo limpo.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    print(f"[broker] clientes em :{FRONTEND_PORT} | servidores em :{BACKEND_PORT}", flush=True)
    try:
        # zmq.proxy fica em loop infinito copiando frontend e backend
        zmq.proxy(frontend, backend)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        frontend.close()
        backend.close()
        ctx.term()


if __name__ == "__main__":
    main()
