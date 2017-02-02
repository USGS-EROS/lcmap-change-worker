FROM ubuntu:17.04
MAINTAINER USGS LCMAP http://eros.usgs.gov

ENV DEBIAN_FRONTEND noninteractive
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV PYTHONIOENCODING UTF-8

# on a centos7 sys, blas-devel and lapack-devel were needed for pips install of scipy
# in ubuntu, those pkgs have different names. though it installed without.
# ubuntu: liblapack-dev libblas-dev   centos: blas-devel lapack-devel
RUN apt-get -y update && apt-get install -y --no-install-recommends \
    apt-utils \
    build-essential \
    ca-certificates \
    curl \
    git \
    libcurl4-openssl-dev \
    openjdk-8-jdk \
    tar \
    wget

RUN locale-gen en_US en_US.UTF-8

RUN dpkg-reconfigure locales

RUN apt-get install -y \
    python \
    python-dev \
    python3 \
    python3-dev

RUN apt-get install -y \
    python-pip \
    python-scipy \
    python-numpy \
    python-pyproj \
    python-h5py \
    python3-scipy \
    python3-numpy \
    python3-pyproj \
    python3-h5py

RUN apt-get update

# MESOS dependencies
RUN apt-get install -y \
    build-essential \
    libapr1-dev \
    libcurl4-nss-dev \
    libsasl2-dev \
    libsasl2-modules \
    libsvn-dev \
    maven \
    zlib1g-dev

# Install pyccd & lcmap-change-worker
RUN pip install -e git+https://github.com/USGS-EROS/lcmap-pyccd.git@develop\#egg=pyccddevelop
RUN pip install -e git+https://github.com/USGS-EROS/lcmap-change-worker.git@develop\#egg=changeworkerdevelop

# Retrieve the Spark package
RUN mkdir -p /opt/spark; cd /opt/spark/; wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz; tar -xf spark-2.0.2-bin-hadoop2.7.tgz;
ENV SPARK_HOME=/opt/spark/spark-2.0.2-bin-hadoop2.7
ENV WORKDIR=/opt/spark/spark-2.0.2-bin-hadoop2.7

# Retrieve pre-compiled Mesos shared libarary
RUN cd /usr/local/lib; wget https://edclpdsftp.cr.usgs.gov/downloads/lcmap/shared_libraries/libmesos-1.1.0.so; ln -s /usr/local/lib/libmesos-1.1.0.so /usr/lib/libmesos.so
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so

WORKDIR /opt/spark/spark-2.0.2-bin-hadoop2.7
