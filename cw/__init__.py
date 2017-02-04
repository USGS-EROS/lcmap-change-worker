from . import messaging
from . import spark
#from messaging.sending import Sending

config = {'rabbit-host': os.getenv('lcw-rabbit-host', 'localhost'),
          'rabbit-port': os.getenv('lcw-rabbit-port', 5672),
          'rabbit-queue': os.getenv('lcw-rabbit-queue': 'unit.lcmap.changes.worker'),
          'rabbit-exchange': os.getenv('lcw-rabbit-exchange', 'unit.lcmap.changes.worker'),
          'rabbit-binding': os.getenv('lcw-rabbit-binding', 'unit.b) 'unit.'',
          'rabbit-ssl': false,
          'ubid_band_dict' : {
              'tm': {'red': 'band3',
                     'blue': 'band1',
                     'green': 'band2',
                     'nirs': 'band4',
                     'swirs1': 'band5',
                     'swirs2': 'band7',
                     'thermals': 'band6',
                     'qas': 'cfmask'},
              'oli': {'red': 'band4',
                      'blue': 'band2',
                      'green': 'band3',
                      'nirs': 'band5',
                      'swirs1': 'band6',
                      'swirs2': 'band7',
                      'thermals': 'band10',
                      'qas': 'cfmask'}}}

def message_receiver(cfg):
    receiver = messaging.Receiving(cfg)
    receiver.start_consuming()

def launch_task(cfg, msg_body):
    # config is a dictionary
    # msg_body needs to be a url
    return spark.run(cfg, msg_body)
    #sender = Sending(config)
    #sender.send(msg_body)
