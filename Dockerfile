FROM alpine:latest

WORKDIR /tmp/

RUN apk update && \
    apk add --no-cache --virtual build-deps gcc python3-dev musl-dev && \
    apk add --no-cache python3 py3-pip py3-pandas postgresql-dev rtl_433 procps

RUN adduser -D super-weather
USER super-weather

WORKDIR /opt/app
RUN pip3 install --user --no-cache --upgrade pip setuptools
RUN pip3 install --user --no-cache  psycopg2 schedule requests
COPY --chown=super-weathe:super-weather . /opt/app

CMD ["python3", "/opt/app/handler.py"]
