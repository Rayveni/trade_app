FROM python:3.9.16-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

#COPY ./topics_config.json /app/

COPY app/requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

RUN rm requirements.txt
COPY app/start.sh /app/
WORKDIR /app/app



