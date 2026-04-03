"""Example: Requeue all — re-queue all received messages to a different channel."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-requeue-all-client",
    ) as client:
        await client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.requeue-source",
                body=b"move-me",
            )
        )

        response = await client.receive_queue_messages(
            channel="python-queues-stream.requeue-source",
            max_messages=10,
            wait_timeout_seconds=5,
        )
        print(f"Received {response.count()} messages from source")
        if not response.is_empty():
            await response.re_queue_all("python-queues-stream.requeue-destination")
            print("All messages re-queued to destination")

        dest = await client.receive_queue_messages(
            channel="python-queues-stream.requeue-destination",
            max_messages=10,
            wait_timeout_seconds=5,
        )
        print(f"Destination has {dest.count()} messages")


if __name__ == "__main__":
    asyncio.run(main())
