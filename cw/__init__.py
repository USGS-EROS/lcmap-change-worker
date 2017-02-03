from . import messaging
from . import spark
#from messaging.sending import Sending

def message_receiver(sysargs):
    receiver = messaging.Receiving(sysargs)
    receiver.start_consuming()

def launch_task(config, msg_body):
    # config is a dictionary
    # msg_body needs to be a url
    return spark.run(config, msg_body)
    #sender = Sending(config)
    #sender.send(msg_body)

ubid_band_dict = {
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
            'qas': 'cfmask'}
}
