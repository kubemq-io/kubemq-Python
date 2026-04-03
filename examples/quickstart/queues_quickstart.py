"""Quick start: Queues — send and receive a message with acknowledgment."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(address="localhost:50000", client_id="python-queues-quickstart-client") as client:
        result = await client.send_queue_message(
            QueueMessage(channel="python-quickstart-queue", body=b"Task #1")
        )
        print(f"Sent: ID={result.id}")

        response = await client.receive_queue_messages(
            channel="python-quickstart-queue",
            max_messages=1,
            wait_timeout_seconds=10,
        )
        for msg in response.messages:
            print(f"Received: {msg.body.decode('utf-8')}")
            await msg.async_ack()


if __name__ == "__main__":
    asyncio.run(main())
