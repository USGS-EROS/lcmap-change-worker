from cw import app, listen, callback

def main():
    listen(app.config, callback(app.config))

if __name__ == "__main__":
    main()
