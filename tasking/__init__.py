#!/usr/bin/env python

import spark


def launch_task(config, args):
    return spark.run(config, args)