"""Example: Expiration policy — send a message that expires after a timeout."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-expiration-policy-client",
    ) as client:
        result = await client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.expiration-policy",
                body=b"message with expiration",
                expiration_in_seconds=5,
            )
        )
        print(f"Sent message with 5s expiration: {result}")

        print("Waiting 6 seconds for expiration...")
        await asyncio.sleep(6)

        response = await client.receive_queue_messages(
            channel="python-queues-stream.expiration-policy",
            max_messages=1,
            wait_timeout_seconds=2,
            auto_ack=True,
        )
        print(f"Received {len(response.messages)} messages (expected 0 — message expired)")


if __name__ == "__main__":
    asyncio.run(main())
