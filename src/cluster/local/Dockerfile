FROM python:3.7

WORKDIR /usr/src/app

# https://github.com/confluentinc/confluent-kafka-python/issues/65#issuecomment-269964346
RUN git clone https://github.com/edenhill/librdkafka \
  && cd librdkafka \
  && git checkout HEAD

WORKDIR librdkafka

RUN ./configure \
  && make \
  && make install \
  && ldconfig  

WORKDIR ..

RUN apt update \
  && apt-get --assume-yes install software-properties-common

RUN wget -qO - https://packages.confluent.io/deb/6.1/archive.key | apt-key add - \
  && add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/6.1 stable main" \
  && apt-get update && apt-get install confluent-librdkafka-plugins 

COPY ./requirements.txt ./

RUN pip install -r requirements.txt

ENV TOPIC_NAME='delivery_event'

COPY . .

CMD python3 -u ./consumer.py
