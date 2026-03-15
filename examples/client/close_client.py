"""Demonstrate graceful client close."""

from kubemq.pubsub import Client, EventMessage


def main():
    client = Client(address="localhost:50000", client_id="close-example")
    try:
        client.publish_event(
            EventMessage(channel="close-demo", body=b"Hello before close")
        )
        print("Event sent successfully")
    finally:
        client.close()
        print("Client closed gracefully")


if __name__ == "__main__":
    main()
