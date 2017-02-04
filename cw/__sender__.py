import cw
import sys

def main(message):
    cw.send(cw.config, message)

if __name__ == "__main__":
    main(sys.argv[1])
