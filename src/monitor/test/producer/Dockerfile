FROM python:3.7

WORKDIR /usr/src/app
COPY . .

COPY ./requirements.txt ./
RUN pip install -r requirements.txt


CMD python3 -u ./producer.py
