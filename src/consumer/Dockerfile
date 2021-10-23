FROM python:3.7

WORKDIR /usr/src/app
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

ENV CONSUME_ENV='prod'
ENV WRITE_ENV='uat'
ENV GROUP_ID='consumer_capacity_1'
ENV BATCH_BYTES='5000000'
ENV WAIT_TIME_SECS='1' 

RUN mkdir -p /usr/src/data


ENTRYPOINT ["python3", "-u", "generic_consumer.py"]
