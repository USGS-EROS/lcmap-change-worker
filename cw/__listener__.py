import cw


def main():
    cw.listen(cw.config, cw.callback(cw.config))

if __name__ == "__main__":
    main()
