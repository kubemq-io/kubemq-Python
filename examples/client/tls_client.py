from kubemq.pubsub import Client


def main():
    try:
        client = Client(
            address="localhost:50000",
            client_id="client-id",
            tls=True,
            tls_ca_file="ca.pem",
            tls_cert_file="cert.pem",
            tls_key_file="key.pem",
        )
        server_info = client.ping()
        print(server_info)
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    main()
