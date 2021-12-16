FROM python:3.7

WORKDIR /usr/src/app
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT ["/bin/sh", "-c", "python3 -u producer.py"]
