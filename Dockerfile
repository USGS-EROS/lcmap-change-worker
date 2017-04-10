FROM usgseros/mesos-spark:latest
MAINTAINER USGS LCMAP http://eros.usgs.gov

RUN apt-get update && apt-get install -y wget --fix-missing
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
COPY sparkdemo_*ipynb /app/
#preposition numpy with conda to avoid compiling from scratch
RUN wget -O Miniconda3-latest.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh; chmod 755 Miniconda3-latest.sh; 
RUN ./Miniconda3-latest.sh -b;
ENV PATH="/root/miniconda3/bin:${PATH}"
RUN conda config --add channels conda-forge;
RUN conda install python=3.5 numpy scipy pandas cassandra-driver jupyter --yes
RUN pip install -e .
RUN pip install -e .[test]
