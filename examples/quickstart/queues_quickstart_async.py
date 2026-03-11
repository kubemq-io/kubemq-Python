"""Quick start: Async Queues — send and receive a queue message asynchronously."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(address="localhost:50000") as client:
        # Send a message to the queue
        result = await client.send_queue_message(
            QueueMessage(channel="quickstart-queue", body=b"Async Task #1")
        )
        print(f"Sent: ID={result.id}")

        # Receive and acknowledge the message
        response = await client.receive_queue_messages(
            channel="quickstart-queue",
            max_messages=1,
            wait_timeout_in_seconds=10,
        )
        for msg in response.messages:
            print(f"Received: {msg.body.decode('utf-8')}")
            await msg.ack()


if __name__ == "__main__":
    asyncio.run(main())
