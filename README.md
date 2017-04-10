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
