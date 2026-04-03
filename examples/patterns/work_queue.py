"""Example: Work queue pattern — distribute tasks across workers using queues."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def worker(worker_id: int) -> None:
    """Worker that pulls and processes tasks from the queue."""
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id=f"python-patterns-work-queue-worker-{worker_id}",
    ) as client:
        response = await client.receive_queue_messages(
            channel="python-patterns.work-queue",
            max_messages=5,
            wait_timeout_seconds=10,
        )
        for msg in response.messages:
            body = msg.body.decode("utf-8")
            print(f"  [Worker-{worker_id}] Processing: {body}")
            await asyncio.sleep(0.1)
            await msg.async_ack()
            print(f"  [Worker-{worker_id}] Done: {body}")


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-patterns-work-queue-producer",
    ) as client:
        for i in range(10):
            await client.send_queue_message(
                QueueMessage(
                    channel="python-patterns.work-queue",
                    body=f"Task #{i + 1}".encode(),
                )
            )
        print("Enqueued 10 tasks")

    tasks = [asyncio.create_task(worker(i)) for i in range(3)]
    await asyncio.gather(*tasks)
    print("All workers finished")


if __name__ == "__main__":
    asyncio.run(main())
