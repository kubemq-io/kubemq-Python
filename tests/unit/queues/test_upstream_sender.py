"""Tests for UpstreamSender."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import grpc
import pytest

from kubemq.core.exceptions import KubeMQBufferFullError
from kubemq.grpc import (
    QueueMessage as pbQueueMessage,
    QueuesUpstreamRequest,
    QueuesUpstreamResponse,
    SendQueueMessageResult,
)
from kubemq.queues.upstream_sender import UpstreamSender


class FakeRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "connection refused"


def _make_mocks():
    mock_transport = MagicMock()
    mock_transport.is_connected.return_value = True
    mock_logger = MagicMock()
    mock_connection = MagicMock()
    mock_connection.disable_auto_reconnect = False
    mock_connection.get_reconnect_delay.return_value = 0.01
    return mock_transport, mock_logger, mock_connection


def _make_sender(mock_transport, mock_logger, mock_connection, **kwargs):
    with patch("kubemq.queues.upstream_sender.threading.Thread"):
        return UpstreamSender(
            mock_transport, mock_logger, mock_connection, **kwargs
        )


# ==============================================================================
# Send Tests
# ==============================================================================


class TestUpstreamSenderSend:
    """Tests for UpstreamSender.send()."""

    def test_send_returns_response(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        message = pbQueueMessage(MessageID="msg-1", Channel="test-q")

        send_result = SendQueueMessageResult(
            MessageID="msg-1", IsError=False, SentAt=1000000000
        )
        response = QueuesUpstreamResponse(Results=[send_result])

        def provide_response():
            while not sender.response_tracking:
                pass
            req_id = next(iter(sender.response_tracking))
            response.RefRequestID = req_id
            container, event, _ = sender.response_tracking[req_id]
            container["response"] = response
            event.set()

        t = threading.Thread(target=provide_response)
        t.start()
        result = sender.send(message)
        t.join(timeout=2)

        assert result is not None
        assert result.is_error is False

    def test_send_when_disconnected(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_transport.is_connected.return_value = False
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        message = pbQueueMessage(MessageID="msg-1", Channel="test-q")
        result = sender.send(message)

        assert result is not None
        assert result.is_error is True
        assert "not connected" in result.error

    def test_send_when_not_accepting(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)
        sender.allow_new_messages = False

        message = pbQueueMessage(MessageID="msg-1", Channel="test-q")
        result = sender.send(message)

        assert result is not None
        assert result.is_error is True
        assert "not ready" in result.error

    def test_send_timeout(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(
            mock_transport, mock_logger, mock_connection, send_timeout=0.01
        )

        message = pbQueueMessage(MessageID="msg-timeout", Channel="test-q")
        result = sender.send(message)

        assert result is not None
        assert result.is_error is True
        assert "Timeout" in result.error

    def test_send_queue_full(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(
            mock_transport, mock_logger, mock_connection, max_queue_size=1
        )

        sender.sending_queue.put_nowait(
            QueuesUpstreamRequest(RequestID="filler")
        )

        message = pbQueueMessage(MessageID="msg-full", Channel="test-q")
        result = sender.send(message)

        assert result is not None
        assert result.is_error is True
        assert "full" in result.error.lower()


# ==============================================================================
# HandleDisconnection Tests
# ==============================================================================


class TestUpstreamSenderHandleDisconnection:
    """Tests for UpstreamSender._handle_disconnection()."""

    def test_clears_pending_requests(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        container1, event1 = {}, threading.Event()
        container2, event2 = {}, threading.Event()
        sender.response_tracking["req-a"] = (container1, event1, "msg-a")
        sender.response_tracking["req-b"] = (container2, event2, "msg-b")

        sender._handle_disconnection()

        assert sender.response_tracking == {}
        assert sender.allow_new_messages is False

        assert event1.is_set()
        assert event2.is_set()
        resp1 = container1["response"]
        resp2 = container2["response"]
        assert resp1.Results[0].IsError is True
        assert "Disconnected" in resp1.Results[0].Error
        assert resp2.Results[0].IsError is True
        assert "Disconnected" in resp2.Results[0].Error


# ==============================================================================
# RecreateChannel Tests
# ==============================================================================


class TestUpstreamSenderRecreateChannel:
    """Tests for UpstreamSender._recreate_channel()."""

    def test_success(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)
        sender.allow_new_messages = False

        new_stub = MagicMock()
        mock_transport.recreate_channel.return_value = new_stub

        result = sender._recreate_channel()

        assert result is True
        assert sender.clientStub is new_stub
        assert sender.allow_new_messages is True

    def test_auto_reconnect_disabled(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_connection.disable_auto_reconnect = True
        mock_transport.recreate_channel.side_effect = ConnectionError("refused")
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        result = sender._recreate_channel()

        assert result is False
        assert sender.allow_new_messages is False

    def test_generic_failure(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_transport.recreate_channel.side_effect = Exception("unexpected")
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        result = sender._recreate_channel()

        assert result is False


# ==============================================================================
# HandleError Tests
# ==============================================================================


class TestUpstreamSenderHandleError:
    """Tests for UpstreamSender._handle_error()."""

    def test_grpc_error(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_transport.recreate_channel.return_value = MagicMock()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        error = FakeRpcError()
        with patch.object(sender, "_handle_disconnection") as mock_disc, \
             patch.object(sender, "_recreate_channel", return_value=True) as mock_recreate:
            result = sender._handle_error(error, is_grpc_error=True)

        assert result is True
        mock_disc.assert_called_once()
        mock_recreate.assert_called_once()

    def test_generic_error(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        error = RuntimeError("something broke")
        with patch.object(sender, "_handle_disconnection") as mock_disc:
            result = sender._handle_error(error)

        assert result is True
        mock_disc.assert_called_once()

    def test_returns_false_when_unreconnectable(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_connection.disable_auto_reconnect = True
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        error = FakeRpcError()
        with patch.object(sender, "_handle_disconnection"), \
             patch.object(sender, "_recreate_channel", return_value=False):
            result = sender._handle_error(error, is_grpc_error=True)

        assert result is False


# ==============================================================================
# Close Tests
# ==============================================================================


class TestUpstreamSenderClose:
    """Tests for UpstreamSender.close()."""

    def test_close_sets_shutdown(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        sender.close()

        assert sender.shutdown_event.is_set()
        assert sender.allow_new_messages is False


# ==============================================================================
# Queue Utilization Warning Tests
# ==============================================================================


class TestUpstreamSenderQueueUtilization:
    """Tests for send queue utilization warning."""

    def test_send_queue_high_utilization_warning(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(
            mock_transport, mock_logger, mock_connection,
            max_queue_size=10, send_timeout=0.01,
        )
        for i in range(9):
            sender.sending_queue.put_nowait(
                QueuesUpstreamRequest(RequestID=f"filler-{i}")
            )

        message = pbQueueMessage(MessageID="msg-warn", Channel="test-q")
        sender.send(message)

        mock_logger.warning.assert_called()


# ==============================================================================
# GenerateRequests Tests
# ==============================================================================


class TestUpstreamSenderGenerateRequests:
    """Tests for UpstreamSender._generate_requests()."""

    def test_sentinel_exits_generator(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        req = QueuesUpstreamRequest(RequestID="req-1")
        sender.sending_queue.put(req)
        sender.sending_queue.put(None)

        results = list(sender._generate_requests())

        assert len(results) == 1
        assert results[0].RequestID == "req-1"


# ==============================================================================
# ProcessResponses Tests
# ==============================================================================


class TestUpstreamSenderProcessResponses:
    """Tests for UpstreamSender._process_responses()."""

    def test_resolves_tracked_responses(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)
        sender.allow_new_messages = False

        container, event = {}, threading.Event()
        sender.response_tracking["req-1"] = (container, event, "msg-1")

        response = QueuesUpstreamResponse(RefRequestID="req-1")
        sender._process_responses(iter([response]))

        assert event.is_set()
        assert container["response"] is response
        assert sender.allow_new_messages is True

    def test_exits_on_shutdown(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)
        sender.shutdown_event.set()

        container, event = {}, threading.Event()
        sender.response_tracking["req-1"] = (container, event, "msg-1")

        response = QueuesUpstreamResponse(RefRequestID="req-1")
        sender._process_responses(iter([response]))

        assert not event.is_set()


# ==============================================================================
# SendQueueStream Tests
# ==============================================================================


class TestUpstreamSenderSendQueueStream:
    """Tests for UpstreamSender._send_queue_stream()."""

    def test_grpc_error_path(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        stub = MagicMock()
        stub.QueuesUpstream.side_effect = FakeRpcError()
        mock_transport.kubemq_client.return_value = stub

        def handle_and_stop(error, is_grpc_error=False):
            sender.shutdown_event.set()
            return True

        with patch.object(sender, '_handle_error', side_effect=handle_and_stop) as mock_handle:
            sender._send_queue_stream()

        mock_handle.assert_called_once()
        args, kwargs = mock_handle.call_args
        assert isinstance(args[0], grpc.RpcError)
        assert kwargs.get('is_grpc_error') is True

    def test_generic_exception_path(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        stub = MagicMock()
        stub.QueuesUpstream.side_effect = RuntimeError("boom")
        mock_transport.kubemq_client.return_value = stub

        def handle_and_stop(error, is_grpc_error=False):
            sender.shutdown_event.set()
            return True

        with patch.object(sender, '_handle_error', side_effect=handle_and_stop) as mock_handle:
            sender._send_queue_stream()

        mock_handle.assert_called_once()
        args, kwargs = mock_handle.call_args
        assert isinstance(args[0], RuntimeError)
        assert kwargs.get('is_grpc_error', False) is False

    def test_exits_when_handle_error_returns_false(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        stub = MagicMock()
        stub.QueuesUpstream.side_effect = FakeRpcError()
        mock_transport.kubemq_client.return_value = stub

        with patch.object(sender, '_handle_error', return_value=False) as mock_handle:
            sender._send_queue_stream()

        mock_handle.assert_called_once()
        assert not sender.shutdown_event.is_set()


# ==============================================================================
# RecreateChannel Corrected Tests
# ==============================================================================


class TestUpstreamSenderRecreateChannelCorrected:
    """Corrected test for _recreate_channel generic failure with logging verification."""

    def test_generic_failure_logs_error(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_transport.recreate_channel.side_effect = Exception("unexpected")
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        result = sender._recreate_channel()

        assert result is False
        mock_logger.error.assert_called()
        log_msg = mock_logger.error.call_args[0][0]
        assert "unexpected" in log_msg


# ==============================================================================
# Coverage Gap Tests
# ==============================================================================


class TestUpstreamSenderUnlimitedQueue:
    """Cover branch 121->131: max_queue_size=0 skips utilization check."""

    def test_send_unlimited_queue_no_warning(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(
            mock_transport, mock_logger, mock_connection,
            max_queue_size=0, send_timeout=0.01,
        )
        message = pbQueueMessage(MessageID="msg-unlimited", Channel="test-q")
        sender.send(message)
        mock_logger.warning.assert_not_called()


class TestUpstreamSenderRecreateChannelConnectionErrorEnabled:
    """Cover lines 239-240: ConnectionError with auto_reconnect enabled."""

    def test_connection_error_auto_reconnect_enabled(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_connection.disable_auto_reconnect = False
        mock_transport.recreate_channel.side_effect = ConnectionError("refused")
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        result = sender._recreate_channel()

        assert result is False
        error_calls = [str(c) for c in mock_logger.error.call_args_list]
        assert any("Connection error" in c for c in error_calls)


class TestUpstreamSenderHandleErrorChannelError:
    """Cover lines 264-266: non-gRPC channel error detection."""

    def test_channel_error_triggers_recreate(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        error = Exception("channel closed unexpectedly")
        with patch.object(sender, "_handle_disconnection"), \
             patch.object(sender, "_recreate_channel", return_value=True) as mock_recreate:
            result = sender._handle_error(error)

        assert result is True
        mock_recreate.assert_called_once()
        error_calls = [str(c) for c in mock_logger.error.call_args_list]
        assert any("Channel error" in c for c in error_calls)


class TestUpstreamSenderSendQueueStreamGenericFalse:
    """Cover line 296: generic exception where _handle_error returns False."""

    def test_generic_exception_exits_thread(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        sender = _make_sender(mock_transport, mock_logger, mock_connection)

        stub = MagicMock()
        stub.QueuesUpstream.side_effect = RuntimeError("boom")
        mock_transport.kubemq_client.return_value = stub

        with patch.object(sender, '_handle_error', return_value=False):
            sender._send_queue_stream()

        assert not sender.shutdown_event.is_set()
