# lcmap-change-worker
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

  # lcw-test-send will send stdin to the configured response exchange
  $ lcw-test-send '{"some":"message"}'
```

```python
  # same effect as lcw-listen
  from cw import __listener__
  __listener__.main()

  # useful for sending test messages only.
  from cw import __sender__
  message = '{"x": 123, "y": 456, "algorithm": "pyccd-beta1", "result": "OUTPUT"}'
  __sender__.main()
```

## Configuration
landsat-change-worker is configurable with the following environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `LCW_RABBIT_HOST` | localhost | RabbitMQ Host |
| `LCW_RABBIT_PORT` | 5672      | RabbitMQ Port |
| `LCW_RABBIT_QUEUE` | local.lcmap.changes.worker | Queue for LCW to listen for messages |
| `LCW_RABBIT_EXCHANGE` | local.lcmap.changes.worker | Exchange for LCW to publish messages |
| `LCW_RABBIT_RESULT_ROUTING_KEY` | change-detection-result | Routing key used when publishing change detection result messages |
| `LCW_RABBIT_SSL` | False | Enable/Disable SSL.  True/False |

## Developing & Testing
Get the local environment ready for development and testing.
```bash
   $ git clone git@github.com:usgs-eros/lcmap-change-worker
   $ cd lcmap-change-worker
   $ virtualenv -p python3 .venv
   $ . .venv/bin/activate
   $ pip install -e .[test]
   $ pip install -e .[dev]
```

Run tests:
```bash
   # the right way
   $ python setup.py test

   # alternatively
   $ pytest
```
## Deploying
