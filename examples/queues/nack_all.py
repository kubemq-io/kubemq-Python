"""Reject all received messages at once using the stream API."""

import asyncio

from kubemq.queues import *


async def main():
    async with AsyncClient(address="localhost:50000") as client:
        await client.send_queue_message(
            QueueMessage(channel="nack-all-demo", body=b"will-be-rejected")
        )

        response = await client.receive_queue_messages(
            channel="nack-all-demo",
            max_messages=10,
            wait_timeout_seconds=5,
        )
        print(f"Received {response.count()} messages")
        if not response.is_empty():
            await response.reject_all()
            print("All messages rejected")


if __name__ == "__main__":
    asyncio.run(main())
