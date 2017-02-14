from cw import app, listen, callback

def main():
    conn = None
    try:
        # the pika connection created here, ends up being the connection
        # used for sending messages
        conn = app.open_connection()
        listen(app.config, callback(app.config, conn), conn)
    except Exception as e:
        pass
    finally:
        app.close_connection(conn)

if __name__ == "__main__":
    main()
