"""Tests for EventSender."""

from __future__ import annotations

import queue
import threading
from unittest.mock import MagicMock, patch

import pytest

from kubemq.core.exceptions import KubeMQBufferFullError
from kubemq.grpc import Event, Result
from kubemq.pubsub.event_sender import EventSender


def _make_sender(*, max_queue_size: int = 10_000) -> tuple[EventSender, MagicMock, threading.Event]:
    mock_transport = MagicMock()
    shutdown_event = threading.Event()
    mock_logger = MagicMock()
    mock_connection = MagicMock()

    with patch("kubemq.pubsub.event_sender.threading.Thread"):
        sender = EventSender(
            mock_transport,
            shutdown_event,
            mock_logger,
            mock_connection,
            max_queue_size=max_queue_size,
        )

    return sender, mock_transport, shutdown_event


class TestEventSenderSend:
    def test_send_non_store_event(self):
        sender, _, _ = _make_sender()
        event = Event(EventID="e1", Store=False)

        result = sender.send(event)

        assert result is None
        assert sender.sending_queue.qsize() == 1
        assert sender.sending_queue.get_nowait() is event

    def test_send_store_event(self):
        sender, _, _ = _make_sender()
        event = Event(EventID="e1", Store=True)

        response = Result(EventID="e1", Sent=True, Error="")

        def _simulate_response():
            while True:
                with sender.lock:
                    if "e1" in sender.response_tracking:
                        container, evt = sender.response_tracking["e1"]
                        container["response"] = response
                        evt.set()
                        return

        t = threading.Thread(target=_simulate_response)
        t.start()

        got = sender.send(event)
        t.join(timeout=2)

        assert got is response
        assert got.EventID == "e1"
        assert got.Sent is True

    def test_send_when_disconnected(self):
        sender, _, _ = _make_sender()
        sender.allow_new_messages = False

        with pytest.raises(ConnectionError, match="not connected"):
            sender.send(Event(EventID="e1", Store=False))

    def test_send_queue_full_non_store(self):
        sender, _, _ = _make_sender(max_queue_size=1)
        sender.sending_queue.put_nowait(Event(EventID="filler"))

        with pytest.raises(KubeMQBufferFullError, match="queue is full"):
            sender.send(Event(EventID="overflow", Store=False))


class TestEventSenderHandleDisconnection:
    def test_clears_queue(self):
        sender, _, _ = _make_sender()
        for i in range(5):
            sender.sending_queue.put_nowait(Event(EventID=f"e{i}"))
        assert sender.sending_queue.qsize() == 5

        sender.handle_disconnection()

        assert sender.sending_queue.empty()

    def test_sets_error_on_tracked_responses(self):
        sender, _, _ = _make_sender()

        container1, evt1 = {}, threading.Event()
        container2, evt2 = {}, threading.Event()
        sender.response_tracking["a"] = (container1, evt1)
        sender.response_tracking["b"] = (container2, evt2)

        sender.handle_disconnection()

        assert evt1.is_set()
        assert evt2.is_set()
        assert container1["response"].Sent is False
        assert "Disconnected" in container1["response"].Error
        assert container2["response"].EventID == "b"
        assert len(sender.response_tracking) == 0

    def test_sets_allow_new_messages_false(self):
        sender, _, _ = _make_sender()
        assert sender.allow_new_messages is True

        sender.handle_disconnection()

        assert sender.allow_new_messages is False


class TestEventSenderSendQueueWarning:
    def test_send_queue_high_utilization_warning(self):
        sender, _, _ = _make_sender(max_queue_size=10)
        for i in range(9):
            sender.sending_queue.put_nowait(Event(EventID=f"e{i}"))
        sender.send(Event(EventID="overflow-warn", Store=False))
        sender.logger.warning.assert_called()


class TestEventSenderHandleDisconnectionPendingResponses:
    def test_handle_disconnection_with_pending_responses(self):
        sender, _, _ = _make_sender()
        container, evt = {}, threading.Event()
        sender.response_tracking["pending-1"] = (container, evt)
        sender.handle_disconnection()
        assert evt.is_set()
        assert container["response"].Sent is False
        assert sender.allow_new_messages is False


class TestEventSenderClose:
    def test_close_does_not_error(self):
        sender, _, shutdown_event = _make_sender()
        shutdown_event.set()


# ==============================================================================
# SendEventsStream Tests
# ==============================================================================

import grpc  # noqa: E402


class _FakeRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "connection refused"


class TestEventSenderSendEventsStream:
    """Tests for EventSender.send_events_stream() method."""

    def test_stream_processes_responses(self):
        """Test that responses from the stream resolve tracked events."""
        sender, mock_transport, shutdown_event = _make_sender()
        sender.connection.reconnect_interval_seconds = 0.01

        container, evt = {}, threading.Event()
        sender.response_tracking["e1"] = (container, evt)

        response = Result(EventID="e1", Sent=True, Error="")
        call_count = 0

        def fake_stream(requests):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return iter([response])
            shutdown_event.set()
            return iter([])

        sender.clientStub.SendEventsStream.side_effect = fake_stream
        sender.send_events_stream()

        assert evt.is_set()
        assert container["response"] is response
        assert container["response"].Sent is True

    def test_stream_handles_grpc_error(self):
        """Test gRPC error triggers handle_disconnection and retry."""
        sender, mock_transport, shutdown_event = _make_sender()
        sender.connection.reconnect_interval_seconds = 0.01

        call_count = 0

        def fake_stream(requests):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise _FakeRpcError()
            shutdown_event.set()
            return iter([])

        sender.clientStub.SendEventsStream.side_effect = fake_stream

        with patch("kubemq.pubsub.event_sender.time.sleep") as mock_sleep:
            sender.send_events_stream()

        assert call_count == 2
        sender.logger.debug.assert_called()
        mock_sleep.assert_called_with(0.01)

    def test_stream_handles_generic_exception(self):
        """Test generic exception triggers handle_disconnection and retry."""
        sender, mock_transport, shutdown_event = _make_sender()
        sender.connection.reconnect_interval_seconds = 0.01

        call_count = 0

        def fake_stream(requests):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("something broke")
            shutdown_event.set()
            return iter([])

        sender.clientStub.SendEventsStream.side_effect = fake_stream

        with patch("kubemq.pubsub.event_sender.time.sleep") as mock_sleep:
            sender.send_events_stream()

        assert call_count == 2
        sender.logger.debug.assert_called()
        mock_sleep.assert_called_with(0.01)

    def test_stream_exits_on_shutdown(self):
        """Test stream loop exits when shutdown_event is set."""
        sender, mock_transport, shutdown_event = _make_sender()
        shutdown_event.set()

        sender.send_events_stream()

        sender.clientStub.SendEventsStream.assert_not_called()


# ==============================================================================
# Coverage Gap Tests
# ==============================================================================


class TestEventSenderUnlimitedQueue:
    """Cover branch 64->74: max_queue_size=0 skips utilization check."""

    def test_send_non_store_unlimited_queue(self):
        sender, _, _ = _make_sender(max_queue_size=0)
        event = Event(EventID="e-unlimited", Store=False)
        result = sender.send(event)
        assert result is None
        sender.logger.warning.assert_not_called()


class TestEventSenderStreamResponseEdgeCases:
    """Cover lines 137, 140->135 for untracked response and shutdown break."""

    def test_stream_untracked_response_ignored(self):
        sender, _, shutdown_event = _make_sender()
        sender.connection.reconnect_interval_seconds = 0.01

        untracked = Result(EventID="no-such-id", Sent=True, Error="")
        call_count = 0

        def fake_stream(requests):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return iter([untracked])
            shutdown_event.set()
            return iter([])

        sender.clientStub.SendEventsStream.side_effect = fake_stream
        sender.send_events_stream()

        assert "no-such-id" not in sender.response_tracking

    def test_stream_shutdown_breaks_response_loop(self):
        sender, _, shutdown_event = _make_sender()
        sender.connection.reconnect_interval_seconds = 0.01

        container, evt = {}, threading.Event()
        sender.response_tracking["e2"] = (container, evt)

        r1 = Result(EventID="e1", Sent=True, Error="")
        r2 = Result(EventID="e2", Sent=True, Error="")

        def fake_stream(requests):
            def _responses():
                yield r1
                shutdown_event.set()
                yield r2

            return _responses()

        sender.clientStub.SendEventsStream.side_effect = fake_stream
        sender.send_events_stream()

        assert not evt.is_set()


class TestEventSenderSendRequestsGenerator:
    """Cover lines 121-128: the inner send_requests() generator."""

    def test_generator_yields_queued_items(self):
        sender, _, shutdown_event = _make_sender()
        sender.connection.reconnect_interval_seconds = 0.01

        event = Event(EventID="e1", Store=False)
        sender.sending_queue.put(event)
        captured = []

        def fake_stream(requests):
            for req in requests:
                captured.append(req)
                shutdown_event.set()
                break
            return iter([])

        sender.clientStub.SendEventsStream.side_effect = fake_stream
        sender.send_events_stream()

        assert len(captured) == 1
        assert captured[0].EventID == "e1"

    def test_generator_handles_empty_queue_timeout(self):
        sender, _, shutdown_event = _make_sender()
        sender.connection.reconnect_interval_seconds = 0.01

        empty_count = 0

        def patched_get(timeout=None):
            nonlocal empty_count
            empty_count += 1
            if empty_count >= 2:
                shutdown_event.set()
            raise queue.Empty()

        def fake_stream(requests):
            for _ in requests:
                pass
            return iter([])

        sender.sending_queue.get = patched_get
        sender.clientStub.SendEventsStream.side_effect = fake_stream
        sender.send_events_stream()

        assert empty_count >= 2


class TestEventSenderHandleDisconnectionEmptyRace:
    """Cover lines 103-104: queue.Empty during handle_disconnection drain."""

    def test_handle_disconnection_empty_race_condition(self):
        from unittest.mock import MagicMock as _MagicMock

        sender, _, _ = _make_sender()

        call_count = 0

        def fake_empty():
            nonlocal call_count
            call_count += 1
            return call_count != 1

        sender.sending_queue.empty = fake_empty
        sender.sending_queue.get_nowait = _MagicMock(side_effect=queue.Empty())

        sender.handle_disconnection()

        assert call_count == 2
