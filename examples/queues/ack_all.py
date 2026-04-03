"""Example: Ack all — acknowledge all pending messages in a queue at once."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-queues-ack-all-client",
    ) as client:
        for i in range(10):
            await client.send_queue_message(
                QueueMessage(
                    channel="python-queues.ack-all",
                    body=f"Msg-{i + 1}".encode(),
                )
            )
        print("Sent 10 messages")

        acked = await client.ack_all_queue_messages(
            "python-queues.ack-all", wait_time_seconds=5
        )
        print(f"Acknowledged {acked} messages")


if __name__ == "__main__":
    asyncio.run(main())
