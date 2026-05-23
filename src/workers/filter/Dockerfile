FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
ENV PYTHONPATH=/app

COPY common ./common
COPY workers/base/*.py ./
COPY workers/filter ./ 

ENTRYPOINT ["python3", "filter.py"]