FROM python:3.5
MAINTAINER USGS LCMAP http://eros.usgs.gov

RUN mkdir /app
WORKDIR /app
COPY pw /app/pw
COPY pytest.ini /app
COPY README.md /app
COPY resources /app/resources
COPY setup.cfg /app
COPY setup.py /app
COPY version.py /app
RUN apt-get update && apt-get install -y zip
RUN mkdir -p /opt/spark/; cd /opt/spark/; wget http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz
RUN cd /opt/spark/; tar xf spark-2.1.0-bin-hadoop2.7.tgz; ln -s spark-2.1.0-bin-hadoop2.7 spark
RUN cd /opt/spark/spark/python/lib; unzip pyspark.zip;  unzip py4j-0.10.4-src.zip
ENV PYTHONPATH=/opt/spark/spark/python/lib
COPY libmesos-1.1.0.so /usr/local/lib/
RUN ln -s /usr/local/lib/libmesos-1.1.0.so /usr/lib/libmesos.so
RUN pip install --upgrade pip
RUN pip install -e.
ENV SPARK_HOME=/opt/spark/spark-2.0.2-bin-hadoop2.7
ENV WORKDIR=/opt/spark/spark-2.0.2-bin-hadoop2.7
WORKDIR /opt/spark/spark-2.0.2-bin-hadoop2.7
