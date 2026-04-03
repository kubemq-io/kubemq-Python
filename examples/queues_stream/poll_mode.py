"""Example: Poll mode — single-shot polling pattern for queue messages."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-poll-mode-client",
    ) as client:
        await client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.poll-mode",
                body=b"message for polling",
            )
        )
        print("Sent 1 message")

        response = await client.receive_queue_messages(
            channel="python-queues-stream.poll-mode",
            max_messages=10,
            wait_timeout_seconds=5,
            auto_ack=True,
        )

        if response.is_error:
            print(f"Error: {response.error}")
            return

        if response.is_empty():
            print("No messages available")
            return

        print(f"Polled {response.count()} messages:")
        for msg in response.messages:
            print(f"  id={msg.id}, body={msg.body.decode('utf-8')}")


if __name__ == "__main__":
    asyncio.run(main())
