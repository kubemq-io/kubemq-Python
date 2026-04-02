"""Example: Work queue pattern — distribute tasks across workers using queues."""

from __future__ import annotations

import time
import threading

from kubemq.queues import Client as QueuesClient
from kubemq import QueueMessage


def worker(worker_id: int) -> None:
    """Worker that pulls and processes tasks from the queue."""
    with QueuesClient(
        address="localhost:50000",
        client_id=f"python-patterns-work-queue-worker-{worker_id}",
    ) as client:
        response = client.receive_queue_messages(
            channel="python-patterns.work-queue",
            max_messages=5,
            wait_timeout_in_seconds=10,
        )
        for msg in response.messages:
            body = msg.body.decode("utf-8")
            print(f"  [Worker-{worker_id}] Processing: {body}")
            time.sleep(0.1)  # Simulate work
            msg.ack()
            print(f"  [Worker-{worker_id}] Done: {body}")


def main() -> None:
    # Producer: enqueue tasks
    with QueuesClient(
        address="localhost:50000",
        client_id="python-patterns-work-queue-producer",
    ) as client:
        for i in range(10):
            client.send_queue_message(
                QueueMessage(
                    channel="python-patterns.work-queue",
                    body=f"Task #{i + 1}".encode(),
                )
            )
        print("Enqueued 10 tasks")

    # Start multiple workers to process tasks concurrently
    threads = []
    for worker_id in range(3):
        t = threading.Thread(target=worker, args=(worker_id,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("All workers finished")


if __name__ == "__main__":
    main()
