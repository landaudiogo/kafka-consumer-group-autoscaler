FROM python:3.7

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV CONTROLLER_ENV=''
ENV GENERAL_LOGGER_LEVEL='INFO'
ENV STATE_MACHINE_LOGGER_LEVEL='DEBUG'

ENTRYPOINT ["python3", "main.py"]
