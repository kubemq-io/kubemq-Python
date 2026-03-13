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
