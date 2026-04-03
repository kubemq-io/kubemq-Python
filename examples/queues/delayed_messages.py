"""Example: Delayed messages — send a message that becomes available after a delay."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-queues-delayed-messages-client",
    ) as client:
        result = await client.send_queue_message(
            QueueMessage(
                channel="python-queues.delayed-messages",
                body=b"message with delay",
                delay_in_seconds=5,
            )
        )
        print(f"Sent delayed message: {result}")

        response = await client.receive_queue_messages(
            channel="python-queues.delayed-messages",
            max_messages=1,
            wait_timeout_seconds=1,
            auto_ack=True,
        )
        print(f"Immediate poll: {len(response.messages)} messages (expected 0)")

        print("Waiting 6 seconds for delay to expire...")
        await asyncio.sleep(6)

        response = await client.receive_queue_messages(
            channel="python-queues.delayed-messages",
            max_messages=1,
            wait_timeout_seconds=5,
            auto_ack=True,
        )
        print(f"After delay: {len(response.messages)} messages (expected 1)")
        for msg in response.messages:
            print(f"  Received: {msg.body.decode('utf-8')}")


if __name__ == "__main__":
    asyncio.run(main())
