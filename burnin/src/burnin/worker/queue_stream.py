"""Queue Stream worker: send_queue_message + receive_queue_messages with manual ack.

v2: Accepts channel_name, rate, channel_index from PatternGroup.
Worker IDs: {role}-{pattern}-{4-digit-channel-index}-{3-digit-worker-index}

Async version: uses AsyncQueuesClient with await.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from kubemq import AsyncQueuesClient, QueueMessage

from burnin import metrics_collector as mc
from burnin import payload
from burnin.worker.base import BaseWorker

if TYPE_CHECKING:
    from burnin.config import Config, PatternConfig

logger = logging.getLogger("burnin")

SDK = "python"
PATTERN = "queue_stream"


class QueueStreamWorker(BaseWorker):
    """Queue Stream pattern: send + receive with manual ack."""

    def __init__(
        self,
        cfg: Config,
        client: AsyncQueuesClient,
        channel_name: str,
        rate: int,
        channel_index: int,
        pattern_config: PatternConfig,
        run_id: str = "",
    ) -> None:
        super().__init__(
            pattern=PATTERN,
            cfg=cfg,
            channel_name=channel_name,
            rate=rate,
            channel_index=channel_index,
        )
        self._client = client
        self._run_id = run_id
        self._pattern_config = pattern_config

    def set_client(self, client: AsyncQueuesClient) -> None:
        self._client = client

    async def start(self) -> None:
        await self.start_consumers_only()
        await self.start_producers_only()

    async def start_consumers_only(self) -> None:
        for i in range(self._pattern_config.consumers_per_channel):
            consumer_id = f"c-{PATTERN}-{self.channel_index:04d}-{i:03d}"
            self._start_task(f"queue-stream-consumer-{self.channel_index:04d}-{i}", self._run_consumer(consumer_id))
        self._consumer_ready.set()

    async def start_producers_only(self) -> None:
        for i in range(self._pattern_config.producers_per_channel):
            producer_id = f"p-{PATTERN}-{self.channel_index:04d}-{i:03d}"
            self._start_task(f"queue-stream-producer-{self.channel_index:04d}-{i}", self._run_producer(producer_id))

    async def _run_consumer(self, consumer_id: str) -> None:
        """Consumer task: poll for messages with manual ack."""
        mc.set_active_connections(PATTERN, 1)
        logger.info("queue_stream consumer %s started on %s", consumer_id, self.channel_name)

        while not self._consumer_stop.is_set():
            try:
                response = await self._client.receive_queue_messages_fast(
                    channel=self.channel_name,
                    max_messages=self.cfg.queue.poll_max_messages,
                    wait_timeout_seconds=self.cfg.queue.poll_wait_timeout_seconds,
                    auto_ack=False,
                )

                if response.is_error:
                    if "timeout" not in str(response.error).lower():
                        logger.debug("queue_stream receive error: %s", response.error)
                        self.record_error("receive_failure")
                    continue

                if not response.messages:
                    continue

                for msg in response.messages:
                    body = msg.body
                    tags = msg.tags or {}
                    crc_tag = tags.get("content_hash", "")

                    if tags.get("warmup") == "true":
                        try:
                            await msg.async_ack()
                        except Exception:
                            pass
                        continue

                    try:
                        if self._benchmark:
                            seq_r, _ = payload.decode_fast(body)
                            self.record_receive(consumer_id, body, "", "", seq_r)
                        else:
                            decoded = payload.decode(body)
                            self.record_receive(
                                consumer_id, body, crc_tag,
                                decoded.producer_id, decoded.sequence,
                            )
                        await msg.async_ack()
                    except Exception:
                        self.record_error("decode_failure")
                        try:
                            await msg.async_nack()
                        except Exception:
                            pass

            except Exception as e:
                if not self._consumer_stop.is_set():
                    logger.debug("queue_stream poll error: %s", e)
                    self.record_error("receive_failure")
                    await asyncio.sleep(1.0)

        mc.set_active_connections(PATTERN, 0)
        logger.info("queue_stream consumer %s stopped", consumer_id)

    async def _run_producer(self, producer_id: str) -> None:
        """Producer task: send queue messages."""
        seq = 0
        logger.info("queue_stream producer %s started on %s", producer_id, self.channel_name)

        while not self._producer_stop.is_set():
            if not await self.wait_for_rate():
                break

            if self.backpressure_check():
                await asyncio.sleep(0.1)
                continue

            seq += 1
            size = self.message_size()
            if self._benchmark:
                body = payload.encode_fast(seq, size)
                crc_hex = ""
            else:
                body, crc_hex = payload.encode(SDK, PATTERN, producer_id, seq, size)

            msg = QueueMessage(
                channel=self.channel_name,
                body=body,
                tags={"content_hash": crc_hex},
            )

            try:
                if self._benchmark:
                    result = await self._client.send_queue_message_fast(msg)
                    if not result.is_error:
                        self.record_send(producer_id, seq, len(body))
                    else:
                        logger.debug("queue_stream send error: %s", result.error)
                        self.record_error("send_result_error")
                else:
                    t0 = time.monotonic()
                    result = await self._client.send_queue_message_fast(msg)
                    duration = time.monotonic() - t0
                    mc.observe_send_duration(PATTERN, duration)

                    if not result.is_error:
                        self.record_send(producer_id, seq, len(body))
                    else:
                        logger.debug("queue_stream send error: %s", result.error)
                        self.record_error("send_result_error")
            except Exception as e:
                logger.debug("queue_stream send error: %s", e)
                self.record_error("send_failure")

        logger.info("queue_stream producer %s stopped", producer_id)
