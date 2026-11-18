# Contexto de build = raiz do projeto (ver docker-compose.yml)
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pyzmq
COPY broker/broker.py .
# -u tira o buffer, talvez tirar
CMD ["python", "-u", "broker.py"]
