"""Queries worker: RPC request/response with send_query + subscribe_to_queries.

v2: Accepts channel_name, rate, channel_index from PatternGroup.
Worker IDs: {role}-{pattern}-{4-digit-channel-index}-{3-digit-worker-index}

Async version: uses AsyncCQClient, AsyncCancellationToken, async iteration.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from kubemq import (
    AsyncCQClient,
    AsyncCancellationToken,
    QueriesSubscription,
    QueryMessage,
    QueryResponse,
)

from burnin import metrics_collector as mc
from burnin import payload
from burnin.worker.base import BaseWorker

if TYPE_CHECKING:
    from burnin.config import Config, PatternConfig

logger = logging.getLogger("burnin")

SDK = "python"
PATTERN = "queries"
RESPONDER_CONCURRENCY = 64

_NOOP_QUERY_CB = lambda _: None


class QueriesWorker(BaseWorker):
    """Queries pattern: RPC request/response with response body verification."""

    def __init__(
        self,
        cfg: Config,
        client: AsyncCQClient,
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

    def set_client(self, client: AsyncCQClient) -> None:
        self._client = client

    async def start(self) -> None:
        await self.start_consumers_only()
        await self.start_producers_only()

    async def start_consumers_only(self) -> None:
        for i in range(self._pattern_config.responders_per_channel):
            responder_id = f"r-{PATTERN}-{self.channel_index:04d}-{i:03d}"
            self._start_task(f"queries-responder-{self.channel_index:04d}-{i}", self._run_responder(responder_id))
        try:
            await asyncio.wait_for(self._consumer_ready.wait(), timeout=30)
        except asyncio.TimeoutError:
            logger.warning("queries responder not ready after 30s on %s", self.channel_name)

    async def start_producers_only(self) -> None:
        for i in range(self._pattern_config.senders_per_channel):
            sender_id = f"s-{PATTERN}-{self.channel_index:04d}-{i:03d}"
            self._start_task(f"queries-sender-{self.channel_index:04d}-{i}", self._run_sender(sender_id))

    def stop_consumers(self) -> None:
        for token in self._cancel_tokens:
            token.cancel()
        super().stop_consumers()

    async def _run_responder(self, responder_id: str) -> None:
        """Responder task: subscribe to queries and auto-respond concurrently.

        Sends back the CRC hash as a minimal body (~8 bytes) instead of echoing
        the full request body (~1KB), reducing per-RPC overhead significantly.
        """
        cancel = AsyncCancellationToken()
        self._cancel_tokens.append(cancel)

        sub = QueriesSubscription(
            channel=self.channel_name,
            group=None,
            on_receive_query_callback=_NOOP_QUERY_CB,
        )

        sem = asyncio.Semaphore(RESPONDER_CONCURRENCY)

        async def _respond(query) -> None:
            async with sem:
                try:
                    crc_tag = (query.tags or {}).get("content_hash", "")
                    resp = QueryResponse(
                        query_received=query,
                        is_executed=True,
                        body=crc_tag.encode(),
                    )
                    await self._client.send_response_fast(resp)
                    self.inc_responder_responded(responder_id)
                except Exception as e:
                    logger.debug("queries response send error: %s", e)
                    self.inc_responder_error(responder_id)
                    self.record_error("response_send_failure")

        mc.set_active_connections(PATTERN, 1)
        self._consumer_ready.set()
        logger.info("queries responder %s started on %s", responder_id, self.channel_name)

        try:
            async for query in self._client.subscribe_to_queries_fast(sub, cancel):
                if self._consumer_stop.is_set():
                    break

                tags = query.tags or {}
                if tags.get("warmup") == "true":
                    try:
                        crc_tag = tags.get("content_hash", "")
                        resp = QueryResponse(
                            query_received=query,
                            is_executed=True,
                            body=crc_tag.encode(),
                        )
                        await self._client.send_response_fast(resp)
                    except Exception:
                        pass
                    continue

                asyncio.create_task(_respond(query))
        except Exception as e:
            if not self._consumer_stop.is_set():
                logger.error("queries subscription error: %s", e)
                self.record_error("subscription_error")
                self.inc_reconnection()
        finally:
            cancel.cancel()
            mc.set_active_connections(PATTERN, 0)

    async def _run_sender(self, sender_id: str) -> None:
        """Sender task: send queries, verify response body CRC, measure RPC latency."""
        seq = 0
        timeout_sec = self.cfg.rpc.timeout_ms / 1000
        logger.info("queries sender %s started on %s", sender_id, self.channel_name)

        while not self._producer_stop.is_set():
            if not await self.wait_for_rate():
                break

            seq += 1
            size = self.message_size()
            if self._benchmark:
                body = payload.encode_fast(seq, size)
                crc_hex = ""
            else:
                body, crc_hex = payload.encode(SDK, PATTERN, sender_id, seq, size)

            query = QueryMessage(
                channel=self.channel_name,
                body=body,
                tags={"content_hash": crc_hex},
                timeout_in_seconds=int(timeout_sec),
            )

            try:
                if self._benchmark:
                    resp = await self._client.send_query_fast(query)
                    if resp.error:
                        error_str = str(resp.error)
                        if "timeout" in error_str.lower() or "rpc timeout" in error_str.lower():
                            self.inc_rpc_timeout(sender_id)
                        else:
                            self.inc_rpc_error(sender_id)
                    else:
                        self.inc_rpc_success(sender_id)
                        self.record_send(sender_id, seq, len(body))
                else:
                    t0 = time.monotonic()
                    resp = await self._client.send_query_fast(query)
                    rpc_duration = time.monotonic() - t0

                    self.record_rpc_latency(rpc_duration)
                    mc.observe_rpc_duration(PATTERN, rpc_duration)

                    if resp.error:
                        error_str = str(resp.error)
                        if "timeout" in error_str.lower() or "rpc timeout" in error_str.lower():
                            self.inc_rpc_timeout(sender_id)
                        else:
                            self.inc_rpc_error(sender_id)
                    else:
                        self.inc_rpc_success(sender_id)
                        self.record_send(sender_id, seq, len(body))

                        resp_body = resp.body if hasattr(resp, "body") and resp.body else b""
                        if resp_body:
                            if resp_body == crc_hex.encode():
                                mc.inc_received(PATTERN, sender_id, len(body))
                            elif payload.verify_crc(resp_body, crc_hex):
                                mc.inc_received(PATTERN, sender_id, len(resp_body))
                            else:
                                self._corrupted += 1
                                mc.inc_corrupted(PATTERN)
            except Exception as e:
                error_str = str(e).lower()
                if "timeout" in error_str:
                    self.inc_rpc_timeout(sender_id)
                else:
                    self.inc_rpc_error(sender_id)
                    logger.debug("queries send error: %s", e)
                self.record_error("send_failure")

        logger.info("queries sender %s stopped", sender_id)
