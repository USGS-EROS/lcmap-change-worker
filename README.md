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
