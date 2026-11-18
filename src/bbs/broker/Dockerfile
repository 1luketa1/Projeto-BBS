# Contexto de build = raiz do projeto (ver docker-compose.yml)
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pyzmq
COPY broker/broker.py .
# -u = saída sem buffer, para os logs aparecerem em tempo real no "docker compose logs"
CMD ["python", "-u", "broker.py"]
