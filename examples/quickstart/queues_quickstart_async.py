"""Quick start: Async Queues — send and receive a queue message asynchronously."""

from __future__ import annotations

import asyncio

from kubemq.queues import AsyncClient as AsyncQueuesClient
from kubemq import QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(address="localhost:50000", client_id="python-queues-quickstart-async-client") as client:
        # Send a message to the queue
        result = await client.send_queue_message(
            QueueMessage(channel="python-quickstart-queue-async", body=b"Async Task #1")
        )
        print(f"Sent: ID={result.id}")

        # Receive the message (simple receive auto-completes the transaction)
        response = await client.receive_queue_messages(
            channel="python-quickstart-queue-async",
            max_messages=1,
            wait_timeout_seconds=10,
        )
        for msg in response.messages:
            print(f"Received: {msg.body.decode('utf-8')}")


if __name__ == "__main__":
    asyncio.run(main())
