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
  # lcw-listen is available following install with pip
  $ lcw-listen

  # lcw-test-send will send stdin to the LCW_RABBIT_EXCHANGE
  $ lcw-test-send '{"some":"message"}'
```

## Configuration
landsat-change-worker is configurable with the following environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `LPW_RABBIT_HOST` | localhost | RabbitMQ Host |
| `LPW_RABBIT_PORT` | 5672      | RabbitMQ Port |
| `LPW_RABBIT_QUEUE` | local.lcmap.changes.worker | Queue for LCW to listen for messages |
| `LPW_RABBIT_EXCHANGE` | local.lcmap.changes.worker | Exchange for LCW to publish messages |
| `LPW_RABBIT_SSL` | False | Enable/Disable SSL.  True/False |
| `LPW_LOG_LEVEL` | INFO | Logging Level.  INFO/DEBUG/WARNING/ERROR/CRITICAL |

## Developing & Testing
Get the local environment ready for development and testing.
```bash
   $ git clone git@github.com:usgs-eros/lcmap-change-worker
   $ cd lcmap-change-worker
   $ git submodule init
   $ git submodule update
   $ virtualenv -p python3 .venv
   $ . .venv/bin/activate
   $ pip install -e .[test]
   $ pip install -e .[dev]
```

Run tests:
```bash
   $ pytest
```
## Deploying
