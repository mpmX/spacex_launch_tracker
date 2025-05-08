FROM python:3.12-slim

RUN apt update && apt install -y curl

WORKDIR /src

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
