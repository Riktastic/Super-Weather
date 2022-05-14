FROM debian:latest

WORKDIR /tmp/

RUN apt-get update
RUN apt-get install -y git libtool libusb-1.0.0-dev librtlsdr-dev rtl-sdr cmake automake python3 python3-pip
RUN pip3 install paho-mqtt numpy
RUN git clone https://github.com/merbanan/rtl_433.git

COPY build.sh .
COPY rtl_433_events.py .
COPY utils.py .

# Build rtl_433
RUN ./build.sh

COPY requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app
RUN pip install -r requirements.txt
COPY . /opt/app

CMD ["python3", "/opt/app/handler.py"]