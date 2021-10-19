FROM python:3.7

WORKDIR /usr/src/app
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

ENV BQ_TABLE='delivery_events'
ENV IMPORT_PATH='contracts.delivery.delivery_events_v6'
ENV IMPORT_OBJECT='DeliveryEventsV6Topic'
ENV IGNORE_EVENTS='"[\"ShippingPricesEstimatedEvent\"]"'
ENV CONSUME_ENV='prod'
ENV WRITE_ENV='uat'
ENV GROUP_ID='testing_consumer_3'
ENV BATCH_BYTES='5000000'
ENV WAIT_TIME_SECS='1' 

RUN mkdir -p /usr/src/data


ENTRYPOINT ["/bin/sh", "-c", "python3 -u generic_consumer.py"]
