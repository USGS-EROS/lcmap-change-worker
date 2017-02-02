from messaging import message_receiver
import sys


def main():
    # required initialization values coming from STDIN
    # initiate listener
    # from STDIN, looking for rabbitmqhost, rabbitmqport, rabbitmqchangequeue, marathonhost, sparkhome
    message_receiver(sys.argv[1])


def run(args):
    message_receiver(args)



if __name__ == "__main__":
    main()
