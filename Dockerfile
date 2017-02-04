FROM jmorton/inferno:latest
MAINTAINER USGS LCMAP http://eros.usgs.gov

ENV version 0.1.0

ENV LCW_RABBIT_HOST rabbitmq
ENV LCW_RABBIT_PORT 5672
ENV LCW_RABBIT_QUEUE local.lcmap.changes.worker
ENV LCW_RABBIT_EXCHANGE	local.lcmap.changes.worker
ENV LCW_RABBIT_SSL False
ENV LCW_RABBIT_RESULT_ROUTING_KEY change-detection-result

RUN mkdir /app
WORKDIR /app
COPY cw /app/cw
COPY pytest.ini /app
COPY README.md /app
COPY resources /app/resources
COPY setup.cfg /app
COPY setup.py /app
COPY version.py /app
RUN pip2 install --upgrade pip
RUN pip2 install -e.

ENTRYPOINT ["lcw-listen"]
