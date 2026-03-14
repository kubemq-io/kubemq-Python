"""Re-queue all received messages to a different channel."""

import asyncio

from kubemq.queues import *


async def main():
    async with AsyncClient(address="localhost:50000") as client:
        await client.send_queue_message(
            QueueMessage(channel="requeue-source", body=b"move-me")
        )

        response = await client.receive_queue_messages(
            channel="requeue-source",
            max_messages=10,
            wait_timeout_seconds=5,
        )
        print(f"Received {response.count()} messages from source")
        if not response.is_empty():
            await response.re_queue_all("requeue-destination")
            print("All messages re-queued to 'requeue-destination'")

        dest = await client.receive_queue_messages(
            channel="requeue-destination",
            max_messages=10,
            wait_timeout_seconds=5,
        )
        print(f"Destination has {dest.count()} messages")


if __name__ == "__main__":
    asyncio.run(main())
