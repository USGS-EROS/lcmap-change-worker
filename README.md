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
  # lpw-listen is available following install with pip
  $ lpw-listen

  # lpw-test-send will send stdin to the LPW_RABBIT_EXCHANGE
  $ lpw-test-send '{"some":"message"}'
```

## Configuration
landsat-pyccd-worker is configurable with the following environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `LPW_RABBIT_HOST` | localhost | RabbitMQ Host |
| `LPW_RABBIT_PORT` | 5672      | RabbitMQ Port |
| `LPW_RABBIT_QUEUE` | local.lcmap.pyccd.worker | Queue for LPW to listen for messages |
| `LPW_RABBIT_EXCHANGE` | local.lcmap.pyccd.worker | Exchange for LPW to publish messages |
| `LPW_RABBIT_SSL` | False | Enable/Disable SSL.  True/False |
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
   $ LPW_RABBIT_HOST=localhost LPW_RABBIT_EXCHANGE=test.lcmap.changes.worker LPW_RABBIT_QUEUE=test.lcmap.changes.worker LPW_RABBIT_RESULT_ROUTING_KEY=change-detection_result pytest
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
Also available from PYPI
```bash
pip install lcmap-pyccd-worker
```
