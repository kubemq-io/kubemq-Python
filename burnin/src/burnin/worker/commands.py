"""Commands worker: RPC request/response with send_command + subscribe_to_commands.

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
    CommandMessage,
    CommandResponse,
    CommandsSubscription,
)

from burnin import metrics_collector as mc
from burnin import payload
from burnin.worker.base import BaseWorker

if TYPE_CHECKING:
    from burnin.config import Config, PatternConfig

logger = logging.getLogger("burnin")

SDK = "python"
PATTERN = "commands"
RESPONDER_CONCURRENCY = 64

_NOOP_CMD_CB = lambda _: None


class CommandsWorker(BaseWorker):
    """Commands pattern: RPC request/response."""

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
            self._start_task(f"commands-responder-{self.channel_index:04d}-{i}", self._run_responder(responder_id))
        try:
            await asyncio.wait_for(self._consumer_ready.wait(), timeout=30)
        except asyncio.TimeoutError:
            logger.warning("commands responder not ready after 30s on %s", self.channel_name)

    async def start_producers_only(self) -> None:
        for i in range(self._pattern_config.senders_per_channel):
            sender_id = f"s-{PATTERN}-{self.channel_index:04d}-{i:03d}"
            self._start_task(f"commands-sender-{self.channel_index:04d}-{i}", self._run_sender(sender_id))

    def stop_consumers(self) -> None:
        for token in self._cancel_tokens:
            token.cancel()
        super().stop_consumers()

    async def _run_responder(self, responder_id: str) -> None:
        """Responder task: subscribe to commands and auto-respond concurrently."""
        cancel = AsyncCancellationToken()
        self._cancel_tokens.append(cancel)

        sub = CommandsSubscription(
            channel=self.channel_name,
            group=None,
            on_receive_command_callback=_NOOP_CMD_CB,
        )

        sem = asyncio.Semaphore(RESPONDER_CONCURRENCY)

        async def _respond(cmd) -> None:
            async with sem:
                try:
                    resp = CommandResponse(
                        command_received=cmd,
                        is_executed=True,
                    )
                    await self._client.send_response_fast(resp)
                    self.inc_responder_responded(responder_id)
                except Exception as e:
                    logger.debug("commands response send error: %s", e)
                    self.inc_responder_error(responder_id)
                    self.record_error("response_send_failure")

        mc.set_active_connections(PATTERN, 1)
        self._consumer_ready.set()
        logger.info("commands responder %s started on %s", responder_id, self.channel_name)

        try:
            async for cmd in self._client.subscribe_to_commands_fast(sub, cancel):
                if self._consumer_stop.is_set():
                    break

                tags = cmd.tags or {}
                if tags.get("warmup") == "true":
                    try:
                        resp = CommandResponse(
                            command_received=cmd,
                            is_executed=True,
                        )
                        await self._client.send_response_fast(resp)
                    except Exception:
                        pass
                    continue

                asyncio.create_task(_respond(cmd))
        except Exception as e:
            if not self._consumer_stop.is_set():
                logger.error("commands subscription error: %s", e)
                self.record_error("subscription_error")
                self.inc_reconnection()
        finally:
            cancel.cancel()
            mc.set_active_connections(PATTERN, 0)

    async def _run_sender(self, sender_id: str) -> None:
        """Sender task: send commands, measure RPC round-trip."""
        seq = 0
        timeout_sec = self.cfg.rpc.timeout_ms / 1000
        logger.info("commands sender %s started on %s", sender_id, self.channel_name)

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

            cmd = CommandMessage(
                channel=self.channel_name,
                body=body,
                tags={"content_hash": crc_hex},
                timeout_in_seconds=int(timeout_sec),
            )

            try:
                if self._benchmark:
                    resp = await self._client.send_command_fast(cmd)
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
                    resp = await self._client.send_command_fast(cmd)
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
                        mc.inc_received(PATTERN, sender_id, len(body))
            except Exception as e:
                error_str = str(e).lower()
                if "timeout" in error_str:
                    self.inc_rpc_timeout(sender_id)
                else:
                    self.inc_rpc_error(sender_id)
                    logger.debug("commands send error: %s", e)
                self.record_error("send_failure")

        logger.info("commands sender %s stopped", sender_id)
