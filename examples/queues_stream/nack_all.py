"""Example: Nack all — reject all received messages at once."""

from __future__ import annotations

import asyncio

from kubemq import AsyncClient, QueueMessage


async def main() -> None:
    async with AsyncClient(
        address="localhost:50000",
        client_id="python-queues-stream-nack-all-client",
    ) as client:
        await client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.nack-all",
                body=b"will-be-rejected",
            )
        )

        response = await client.receive_queue_messages(
            channel="python-queues-stream.nack-all",
            max_messages=10,
            wait_timeout_seconds=5,
        )
        print(f"Received {response.count()} messages")
        if not response.is_empty():
            await response.reject_all()
            print("All messages rejected (nacked)")


if __name__ == "__main__":
    asyncio.run(main())
