FROM python:3.5
MAINTAINER USGS LCMAP http://eros.usgs.gov


RUN mkdir /app
WORKDIR /app
COPY cw /app/cw
COPY pytest.ini /app
COPY README.md /app
COPY resources /app/resources
COPY setup.cfg /app
COPY setup.py /app
COPY version.py /app
RUN pip install --upgrade pip
RUN pip install -e.

ENTRYPOINT ["lpw-listen"]
