#!/usr/bin/env python
import os
import sys
from glob import glob


class Spark(object):
    def __init__(self, config, params):
        self.config = config
        self.params = params
        sys.path.insert(0, os.path.join(self.config['sparkhome'], 'python'))
        sys.path.insert(0, os.path.join(self.config['sparkhome'], glob('python/lib/py4j*.zip')[0]))
        from pyspark import SparkContext, SparkConf

    def run(self, job_args):
        return True


def run(config, params):
    sprk = Spark(config)
    return sprk.run(params)

