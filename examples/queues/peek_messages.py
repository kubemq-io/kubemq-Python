"""Example: Peek messages — inspect queue messages without consuming them."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-queues-peek-messages-client",
    ) as client:
        await client.send_queue_message(
            QueueMessage(
                channel="python-queues.peek-messages",
                body=b"peek-me",
                metadata="test",
            )
        )

        waiting_result = await client.peek_queue_messages("python-queues.peek-messages", 5, 10)
        if waiting_result.is_error:
            print(f"Error: {waiting_result.error}")
            return

        print(f"Messages waiting: {len(waiting_result.messages)}")
        for msg in waiting_result.messages:
            print(f"  Peek: id={msg.id}, body={msg.body.decode('utf-8')}")

        pull_result = await client.receive_queue_messages(
            channel="python-queues.peek-messages",
            max_messages=5,
            wait_timeout_seconds=10,
            auto_ack=True,
        )
        print(f"Messages pulled after peek: {len(pull_result.messages)}")


if __name__ == "__main__":
    asyncio.run(main())
