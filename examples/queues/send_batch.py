"""Send multiple messages as a batch using the batch send API."""

import asyncio

from kubemq.queues import *


async def main():
    async with AsyncClient(address="localhost:50000") as client:
        messages = [
            QueueMessage(channel="batch-demo", body=f"Batch msg #{i + 1}".encode())
            for i in range(5)
        ]
        results = await client.send_queue_messages_batch(messages)
        for r in results:
            print(f"Sent: id={r.id}, sent_at={r.sent_at}")

        poll = await client.receive_queue_messages(
            channel="batch-demo", max_messages=10, wait_timeout_seconds=5
        )
        for msg in poll.messages:
            print(f"Received: {msg.body.decode('utf-8')}")
            await msg.ack()


if __name__ == "__main__":
    asyncio.run(main())
