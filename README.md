[![Build Status](https://travis-ci.org/USGS-EROS/lcmap-pyccd-worker.svg?branch=develop)](https://travis-ci.org/USGS-EROS/lcmap-pyccd-worker)

# lcmap-pyccd-worker
worker for initiating change detection jobs, and sending results to the data store

## Install
```bash
  # locally
  $ pip install -e.
```

## Usage
```bash
```

## Configuration
landsat-pyccd-worker is configurable with the following environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `LPW_LOG_LEVEL` | INFO | Logging Level.  INFO/DEBUG/WARNING/ERROR/CRITICAL |
| `DB_CONTACT_POINTS` | 0.0.0.0 | Host IP for Cassandra instance |
| `DB_KEYSPACE` | '' | Desired Cassandra keyspace |
| `DB_PASSWORD` | '' | Password for Cassandra instance |
| `DB_USERNAME` | '' | Username for Cassandra instance |
| `LPW_MESOS_MASTER` | | Mesos master host name (include port) |
| `LPW_EXECUTOR_IMAGE` | | Docker image to run Spark Executor in |
| `LPW_EXECUTOR_CORES` | | Number of cores alotted to each Spark Executor |
| `LPW_EXECUTOR_FORCE_PULL_IMAGE` | 'true' | Force pull image for Spark Executor |
| `LPW_SPARK_PARALLELIZATION` | | Number of items data should be divided into for parallel execution |
| `PYSPARK_DRIVER_PYTHON` | | Which python the spark driver should use |
| `PYSPARK_PYTHON` | | Which python the spark executor should use |

## Developing & Testing
Get the local environment ready for development and testing.
```bash
   $ git clone git@github.com:usgs-eros/lcmap-pyccd-worker
   $ cd lcmap-pyccd-worker
   $ git submodule init
   $ git submodule update
   $ virtualenv -p python3 .venv
   $ . .venv/bin/activate
   $ pip install -e .[test]
   $ pip install -e .[dev]
```

Run tests:
```bash
   # in a separate shell
   $ make docker-deps-up-no-daemon
   # export required env variables
   # run pytest
   $ pytest
```

All tests run at Docker file build time:

```bash
   $ docker build -t tmp-testing-container .
```

## Deploying
Available from Docker Hub https://hub.docker.com/r/usgseros/lcmap-pyccd-worker/
```bash
docker pull usgseros/lcmap-pyccd-worker
```

Backing services are available from the lcmap-services project, utilizing docker-compose 3
```bash
git clone https://github.com/USGS-EROS/lcmap-services
# You may or may not need to declare the HOSTNAME env variable depending on your OS
cd lcmap-services; HOSTNAME=<yourmachine> docker-compose up
```

The backing mesos-master and mesos-slave from the lcmap-services communicate over the lcmapservices_lcmap
network, when you start an lcmap-pyccd-worker container, you need to tell it to use this network.
You also need to expose ports for accessing any jupyter notebooks initiated from the container.
 ```bash
 docker run -it -p 0.0.0.0:8888:8888 --network lcmapservices_lcmap usgseros/lcmap-pyccd-worker:spark /bin/bash
 ```

Once you are in the lcmap-pyccd-worker container, you'll need to set some configuration values in the
environment before launching either of the provided jupyter notebooks.
```bash
export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_PYTHON=python3
export LPW_MESOS_MASTER=mesos://< the ip address of the mesos-master started by lcmap-services >:5050                                                                                                                                                          
export LPW_EXECUTOR_IMAGE=usgseros/lcmap-pyccd-worker:spark
export LPW_EXECUTOR_CORES=4
export LPW_EXECUTOR_FORCE_PULL_IMAGE=true
export LPW_SPARK_PARALLELIZATION=2
```

Now you'll be ready to start either of the jupyter notebooks.
```bash
jupyter notebook --ip=0.0.0.0 --browser=none --allow-root
```

From the system thats running the lcmap-pyccd-container, you'll be able to access the jupyter notebooks
at http://127.0.0.1:8888
They are named sparkdemo_from_data.ipynb and sparkdemo_from_http.ipynb.  The first operates on data provided
from a submodule under the /data directory when the project was cloned.  The latter will pull tile and spectral
data from a web api if it is available.

The Mesos master UI will be accessible from http://127.0.0.1:5050/



