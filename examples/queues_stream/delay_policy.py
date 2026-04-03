"""Example: Delay policy — send a message with a delivery delay."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-delay-policy-client",
    ) as client:
        result = await client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.delay-policy",
                body=b"delayed message",
                delay_in_seconds=5,
            )
        )
        print(f"Sent message with 5s delay: {result}")

        response = await client.receive_queue_messages(
            channel="python-queues-stream.delay-policy",
            max_messages=1,
            wait_timeout_seconds=1,
            auto_ack=True,
        )
        print(f"Immediate: {len(response.messages)} messages (expected 0)")

        print("Waiting 6 seconds for delay...")
        await asyncio.sleep(6)

        response = await client.receive_queue_messages(
            channel="python-queues-stream.delay-policy",
            max_messages=1,
            wait_timeout_seconds=5,
            auto_ack=True,
        )
        print(f"After delay: {len(response.messages)} messages (expected 1)")
        for msg in response.messages:
            print(f"  Received: {msg.body.decode('utf-8')}")


if __name__ == "__main__":
    asyncio.run(main())
