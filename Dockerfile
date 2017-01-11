FROM ubuntu:14.04
MAINTAINER USGS LCMAP http://eros.usgs.gov

ENV DEBIAN_FRONTEND noninteractive
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV PYTHONIOENCODING UTF-8

RUN apt-get update

RUN apt-get upgrade -y

RUN apt-get install -y --no-install-recommends \
    apt-utils \
    build-essential \
    ca-certificates \
    libcurl4-openssl-dev \
    curl \
    wget \
    git

RUN apt-get install -y software-properties-common python-software-properties

RUN locale-gen en_US en_US.UTF-8

RUN dpkg-reconfigure locales

RUN apt-get install -y python python-dev python3 python3-dev

RUN apt-get install -y python-pip python-scipy python-numpy python-pyproj python-h5py \
    python3-scipy python3-numpy python3-pyproj python3-h5py

RUN apt-get update


