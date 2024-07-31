from kubemq.pubsub import Client


def main():
    try:
        client = Client(address="localhost:50000")
        server_info = client.ping()
        print(server_info)
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    main()
