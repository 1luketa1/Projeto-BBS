FROM python:3.12-slim
WORKDIR /app

RUN pip install --no-cache-dir pyzmq protobuf grpcio-tools

COPY proto/ proto/
RUN python -m grpc_tools.protoc -I proto --python_out=. proto/bbs.proto

COPY server/ .
CMD ["python", "-u", "server.py"]
