"""Example: Stream receive — receive messages with ack/reject/requeue options."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-stream-receive-client",
    ) as client:
        for i in range(3):
            await client.send_queue_message(
                QueueMessage(
                    channel="python-queues-stream.stream-receive",
                    body=f"Message #{i + 1}".encode(),
                )
            )
        print("Sent 3 test messages")

        response = await client.receive_queue_messages(
            channel="python-queues-stream.stream-receive",
            max_messages=10,
            wait_timeout_seconds=5,
        )

        for msg in response.messages:
            body = msg.body.decode("utf-8")
            print(f"Received: {body}")

            if "1" in body:
                await msg.async_ack()
                print(f"  -> Acknowledged: {body}")
            elif "2" in body:
                await msg.async_nack()
                print(f"  -> Rejected: {body}")
            else:
                await msg.async_ack()
                print(f"  -> Acknowledged: {body}")


if __name__ == "__main__":
    asyncio.run(main())
