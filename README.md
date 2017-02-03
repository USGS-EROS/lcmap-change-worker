# lcmap-change-worker
worker for initiating change detection jobs, and sending results to the data store

## Usage

## Install
```bash
  # locally
  $ python setup.py install

  # from pypi
  $ pip install lcmap-change-worker
```
## Configuration

## Developing & Testing

```bash
   $ git clone git@github.com:usgs-eros/lcmap-change-worker
   $ cd lcmap-change-worker
   $ virtualenv -p python3 .venv
   $ . .venv/bin/activate
   $ pip install -e .[test]
   $ pip install -e .[dev]
   # This is temporary until pyccd is pushed into pypi   
   $ pip install -r pyccd-requirement.txt
```

## Deploying
