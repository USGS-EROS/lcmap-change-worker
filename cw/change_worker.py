from cw import message_receiver
import json
import sys

def main():
    # required initialization values coming from STDIN
    # initiate listener
    # from STDIN, looking for rabbitmqhost, rabbitmqport, rabbitmqchangequeue, marathonhost, sparkhome
    message_receiver(json.loads(sys.argv[1]))

if __name__ == "__main__":
    main()
