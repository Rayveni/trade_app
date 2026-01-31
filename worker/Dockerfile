#FROM python:3.9.16-slim-bullseye
FROM python:3.14.2-slim-bookworm
#FROM python:3.13.3-alpine3.22
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get -y install libpq-dev gcc 
#RUN apk add build-base libpq libpq-dev   
COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

WORKDIR /worker