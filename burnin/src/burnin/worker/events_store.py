"""Events Store worker: persistent pub/sub.

v2: Accepts channel_name, rate, channel_index from PatternGroup.
Worker IDs: {role}-{pattern}-{4-digit-channel-index}-{3-digit-worker-index}

Async version: uses AsyncPubSubClient, AsyncCancellationToken, async iteration.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from kubemq import (
    AsyncCancellationToken,
    AsyncPubSubClient,
    EventStoreMessage,
    EventsStoreSubscription,
)
from kubemq.pubsub.events_store_subscription import EventStoreStartPosition

from burnin import metrics_collector as mc
from burnin import payload
from burnin.worker.base import BaseWorker

if TYPE_CHECKING:
    from burnin.config import Config, PatternConfig

logger = logging.getLogger("burnin")

SDK = "python"
PATTERN = "events_store"

_NOOP_EVENT_STORE_CB = lambda _: None


class EventsStoreWorker(BaseWorker):
    """Events Store pattern: persistent pub/sub."""

    def __init__(
        self,
        cfg: Config,
        client: AsyncPubSubClient,
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
        self._cancel_tokens: list[AsyncCancellationToken] = []

    def set_client(self, client: AsyncPubSubClient) -> None:
        self._client = client

    async def start(self) -> None:
        await self.start_consumers_only()
        await self.start_producers_only()

    async def start_consumers_only(self) -> None:
        use_group = self._pattern_config.consumer_group
        for i in range(self._pattern_config.consumers_per_channel):
            consumer_id = f"c-{PATTERN}-{self.channel_index:04d}-{i:03d}"
            group = f"python_burnin_{self._run_id}_{PATTERN}_{self.channel_index:04d}_group" if use_group else ""
            self._start_task(f"events-store-consumer-{self.channel_index:04d}-{i}", self._run_consumer(consumer_id, group))
        try:
            await asyncio.wait_for(self._consumer_ready.wait(), timeout=30)
        except asyncio.TimeoutError:
            logger.warning("events_store consumer not ready after 30s on %s", self.channel_name)

    async def start_producers_only(self) -> None:
        for i in range(self._pattern_config.producers_per_channel):
            producer_id = f"p-{PATTERN}-{self.channel_index:04d}-{i:03d}"
            self._start_task(f"events-store-producer-{self.channel_index:04d}-{i}", self._run_producer(producer_id))

    def stop_consumers(self) -> None:
        for token in self._cancel_tokens:
            token.cancel()
        super().stop_consumers()

    async def _run_consumer(self, consumer_id: str, group: str) -> None:
        """Consumer task: subscribe to events store via async iteration."""
        cancel = AsyncCancellationToken()
        self._cancel_tokens.append(cancel)

        sub = EventsStoreSubscription(
            channel=self.channel_name,
            group=group if group else None,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_receive_event_callback=_NOOP_EVENT_STORE_CB,
        )

        mc.set_active_connections(PATTERN, 1)
        self._consumer_ready.set()
        logger.info("events_store consumer %s started on %s", consumer_id, self.channel_name)

        try:
            async for event in self._client.subscribe_to_events_store_fast(sub, cancel):
                if self._consumer_stop.is_set():
                    break

                body = event.body
                tags = event.tags or {}
                crc_tag = tags.get("content_hash", "")

                if tags.get("warmup") == "true":
                    continue

                try:
                    if self._benchmark:
                        seq_r, _ = payload.decode_fast(body)
                        self.record_receive(consumer_id, body, "", "", seq_r)
                    else:
                        msg = payload.decode(body)
                        self.record_receive(consumer_id, body, crc_tag, msg.producer_id, msg.sequence)
                except Exception:
                    self.record_error("decode_failure")
        except Exception as e:
            if not self._consumer_stop.is_set():
                logger.error("events_store subscription error: %s", e)
                self.record_error("subscription_error")
                self.inc_reconnection()
        finally:
            cancel.cancel()
            mc.set_active_connections(PATTERN, 0)

    async def _run_producer(self, producer_id: str) -> None:
        """Producer task: publish to event store, check .sent result."""
        seq = 0
        logger.info("events_store producer %s started on %s", producer_id, self.channel_name)

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

            msg = EventStoreMessage(
                channel=self.channel_name,
                body=body,
                tags={"content_hash": crc_hex},
            )

            try:
                if self._benchmark:
                    result = await self._client.send_event_store_fast(msg)
                    if result.sent:
                        self.record_send(producer_id, seq, len(body))
                    else:
                        self._unconfirmed += 1
                        logger.debug("events_store send not confirmed: %s", result.error)
                        self.record_error("send_result_error")
                else:
                    t0 = time.monotonic()
                    result = await self._client.send_event_store_fast(msg)
                    duration = time.monotonic() - t0
                    mc.observe_send_duration(PATTERN, duration)

                    if result.sent:
                        self.record_send(producer_id, seq, len(body))
                    else:
                        self._unconfirmed += 1
                        logger.debug("events_store send not confirmed: %s", result.error)
                        self.record_error("send_result_error")
            except Exception as e:
                logger.debug("events_store send error: %s", e)
                self.record_error("send_failure")

        logger.info("events_store producer %s stopped", producer_id)
