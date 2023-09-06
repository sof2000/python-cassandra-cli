FROM alpine:3.11

LABEL maintainer="Andrii Derevianko"

WORKDIR /

ADD ./requirements.txt /requirements.txt

RUN apk upgrade --no-cache \
  && apk add --no-cache \
    python3 \
    python3-dev \
  && pip3 install --no-cache-dir --upgrade pip \
  && rm -rf /var/cache/* \
  && rm -rf /root/.cache/* 

RUN pip3 install -r requirements.txt
RUN pip3 install python-cassandra-cli==1.4.0 --no-cache-dir



RUN cd /usr/bin \
  && ln -sf python3 python \
  && ln -sf pip3 pip
