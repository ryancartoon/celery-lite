from tasks.tasks import hello


def main():
    hello.delay()


if __name__ == "__main__":
    main()