"""Example: Ack and reject — demonstrate per-message ack and reject decisions."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-queues-ack-reject-client",
    ) as client:
        for i in range(4):
            await client.send_queue_message(
                QueueMessage(
                    channel="python-queues.ack-reject",
                    body=f"Order-{i + 1}".encode(),
                )
            )
        print("Sent 4 messages")

        response = await client.receive_queue_messages(
            channel="python-queues.ack-reject",
            max_messages=4,
            wait_timeout_seconds=10,
        )
        for msg in response.messages:
            body = msg.body.decode("utf-8")
            if msg.sequence % 2 == 0:
                await msg.async_ack()
                print(f"  Acked: {body} (seq={msg.sequence})")
            else:
                await msg.async_nack()
                print(f"  Rejected: {body} (seq={msg.sequence})")


if __name__ == "__main__":
    asyncio.run(main())
