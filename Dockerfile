FROM usgseros/mesos-spark:latest
MAINTAINER USGS LCMAP http://eros.usgs.gov

# python3-pip install setuptools, which is req'd for install of lcmap-pyccd-worker
# liblapack, libblas & gfortran are needed for scipy.  python3-dev is needed for numpy
RUN apt-get update && apt-get install -y python3-pip python3-dev liblapack-dev libblas-dev gfortran
RUN mkdir /app
WORKDIR /app
COPY pw /app/pw
COPY pytest.ini /app
COPY README.md /app
COPY resources /app/resources
COPY setup.cfg /app
COPY setup.py /app
COPY version.py /app
COPY test /app/test
COPY data /app/data
RUN pip3 install -e .

