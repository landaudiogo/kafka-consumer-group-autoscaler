FROM python:3.7

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

ENV BQ_TABLE='delivery_events_temp'
ENV IMPORT_PATH='contracts.delivery.delivery_events_v10'
ENV IMPORT_OBJECT='DeliveryEventsV10Topic'
ENV IGNORE_EVENTS='"[\"ShippingPricesEstimatedEvent\"]"'
ENV CONSUME_ENV='prod'
ENV WRITE_ENV='uat'
ENV GROUP_ID='BT-test-duplicates'


CMD python3 -u generic_consumer.py
