import receiving
import sending


def message_receiver(sysargs):
    receiver = receiving.Receiving(sysargs)
    receiver.start_consuming()
