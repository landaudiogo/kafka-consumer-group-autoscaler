FROM python:3.7

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

# ENV BQ_TABLE='delivery_events_temp'
# ENV IMPORT_PATH='contracts.delivery.delivery_events_v6'
# ENV IMPORT_OBJECT='DeliveryEventsV6Topic'
# ENV IGNORE_EVENTS='"[\"ShippingPricesEstimatedEvent\"]"'
# ENV CONSUME_ENV='prod'
# ENV WRITE_ENV='uat'
# ENV GROUP_ID='test_generic_delivery10'


CMD python3 -u generic_consumer.py
