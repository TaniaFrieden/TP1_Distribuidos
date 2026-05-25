FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
ENV PYTHONPATH=/app

COPY common ./common
COPY workers/base/*.py ./
COPY workers/joiner_q4 ./

ENTRYPOINT ["python3", "joiner_q4.py"]
