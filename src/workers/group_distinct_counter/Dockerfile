FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
ENV PYTHONPATH=/app

COPY common ./common
COPY workers/base/*.py ./
COPY workers/group_distinct_counter ./

ENTRYPOINT ["python3", "group_distinct_counter.py"]
