import os
import pytest

@pytest.fixture(scope="session")
def set_environment():
    os.environ['LCW_RABBIT_HOST']        = 'localhost'
    os.environ['LCW_RABBIT_PORT']        = '5672'
    os.environ['LCW_RABBIT_QUEUE']       = 'unit.lcmap.changes.worker'
    os.environ['LCW_RABBIT_EXCHANGE']    = 'unit.lcmap.changes.worker'
    os.environ['LCW_RABBIT_SSL']         = 'False'
    os.environ['LCW_TILE_SPEC_HOST']     = 'localhost'
    os.environ['LCW_TILE_SPEC_PORT']     = '5678'
    os.environ['LCW_LOG_LEVEL']          = 'INFO'
    os.environ['LCW_RESULT_ROUTING_KEY'] = 'change-detection-result'
    return os.environ
