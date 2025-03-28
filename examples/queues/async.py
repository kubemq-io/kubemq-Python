import asyncio
import logging
import os
from logging.config import dictConfig



from kubemq.queues import *

async def example_send_receive():
    with Client(
        address="localhost:50000",
    ) as client:
        async def _send():
            while True:
                send_result = await client.send_queues_message_async(
                    QueueMessage(
                        channel="send_receive",
                        body=b"message",
                    ),
                )
                if send_result.is_error:
                    print(f"Error sending message: {send_result.error}")
                    await asyncio.sleep(1)
                    continue
                print(f"Queue Message Sent: {send_result}")
                await asyncio.sleep(1)

        async def _receive():
            while True:
                send_receive_result = await client.receive_queues_messages_async(
                    channel="send_receive",
                    max_messages=1,
                    wait_timeout_in_seconds=1,
                )
                if send_receive_result.is_error:
                    print(f"Error receiving message: {send_receive_result.error}")
                    await asyncio.sleep(1)
                    continue
                for message in send_receive_result.messages:
                    print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
                    message.ack()

                await asyncio.sleep(1)

        task1 = asyncio.create_task(_send())
        task2 = asyncio.create_task(_receive())
        tasks = [task1, task2]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(example_send_receive())

