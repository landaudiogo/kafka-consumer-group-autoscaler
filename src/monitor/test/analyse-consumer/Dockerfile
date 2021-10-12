FROM python:3.7

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

RUN mkdir -p /usr/src/data

CMD python3 -u ./consumer.py
