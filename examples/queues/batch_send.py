"""Example: Batch send — send multiple queue messages in a single batch."""

from __future__ import annotations

import asyncio

from kubemq import AsyncClient, QueueMessage


async def main() -> None:
    async with AsyncClient(
        address="localhost:50000",
        client_id="python-queues-batch-send-client",
    ) as client:
        messages = [
            QueueMessage(
                channel="python-queues.batch-send",
                body=f"Batch msg #{i + 1}".encode(),
            )
            for i in range(5)
        ]
        results = await client.send_queue_messages_batch(messages)
        for r in results:
            print(f"Sent: id={r.id}, sent_at={r.sent_at}")

        poll = await client.receive_queue_messages(
            channel="python-queues.batch-send",
            max_messages=10,
            wait_timeout_seconds=5,
            auto_ack=True,
        )
        for msg in poll.messages:
            print(f"Received: {msg.body.decode('utf-8')}")


if __name__ == "__main__":
    asyncio.run(main())
