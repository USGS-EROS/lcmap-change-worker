FROM ubuntu:17.04

RUN apt-get -y update && apt-get install -y git python-pip openjdk-8-jdk tar wget

# MESOS dependencies
RUN apt-get install -y build-essential python-dev libcurl4-nss-dev libsasl2-dev libsasl2-modules maven libapr1-dev libsvn-dev zlib1g-dev

RUN pip install -e git+https://github.com/USGS-EROS/lcmap-change-worker.git@develop\#egg=changeworkerdevelop

RUN mkdir -p /opt/spark; cd /opt/spark/; wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz; tar -xf spark-2.0.2-bin-hadoop2.7.tgz;

# Retrieve pre-compiled Mesos shared libarary
RUN cd /usr/local/lib; wget https://edclpdsftp.cr.usgs.gov/downloads/lcmap/shared_libraries/libmesos-1.1.0.so; ln -s /usr/local/lib/libmesos-1.1.0.so /usr/lib/libmesos.so
RUN ln -s /usr/local/lib/libmesos-1.1.0.so /usr/lib/libmesos.so
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so

ENV SPARK_HOME=/opt/spark/spark-2.0.2-bin-hadoop2.7
ENV WORKDIR=/opt/spark/spark-2.0.2-bin-hadoop2.7
WORKDIR /opt/spark/spark-2.0.2-bin-hadoop2.7
