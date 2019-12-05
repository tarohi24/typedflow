FROM python:3.8-buster

RUN apt-get update && \
        apt-get install -y gcc build-essential libomp-dev libopenblas-dev

WORKDIR /workplace
ADD requirements.txt /workplace/
RUN pip install -r requirements.txt
ADD requirements_dev.txt /workplace/
RUN pip install -r requirements_dev.txt

ADD setup.py /workplace/
ADD typedflow /workplace/typeflow
RUN pip install --editable .
VOLUME /workplace
