"""Tests for DownstreamReceiver."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import grpc
import pytest

from kubemq.core.exceptions import KubeMQBufferFullError
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
)
from kubemq.queues.downstream_receiver import DownstreamReceiver


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


def _make_receiver(mock_transport, mock_logger, mock_connection, **kwargs):
    with patch("kubemq.queues.downstream_receiver.threading.Thread"):
        return DownstreamReceiver(mock_transport, mock_logger, mock_connection, **kwargs)


# ==============================================================================
# Send Tests
# ==============================================================================


class TestDownstreamReceiverSend:
    """Tests for DownstreamReceiver.send()."""

    def test_send_returns_response(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        request = QueuesDownstreamRequest(RequestID="req-1", WaitTimeout=1000)
        expected_response = QueuesDownstreamResponse(RefRequestId="req-1", IsError=False)

        def provide_response():
            while "req-1" not in receiver.response_tracking:
                pass
            container, event = receiver.response_tracking["req-1"]
            container["response"] = expected_response
            event.set()

        t = threading.Thread(target=provide_response)
        t.start()
        result = receiver.send(request)
        t.join(timeout=2)

        assert result.RefRequestId == "req-1"
        assert result.IsError is False

    def test_send_when_disconnected(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_transport.is_connected.return_value = False
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        request = QueuesDownstreamRequest(RequestID="req-1", WaitTimeout=1000)
        result = receiver.send(request)

        assert result.IsError is True
        assert "not connected" in result.Error

    def test_send_when_not_accepting(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)
        receiver.allow_new_requests = False

        request = QueuesDownstreamRequest(RequestID="req-1", WaitTimeout=1000)
        result = receiver.send(request)

        assert result.IsError is True
        assert "not ready" in result.Error

    def test_send_timeout(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection, timeout_buffer=0.0)

        request = QueuesDownstreamRequest(RequestID="req-timeout", WaitTimeout=10)
        result = receiver.send(request)

        assert result.IsError is True
        assert "Timeout" in result.Error

    def test_send_queue_full(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection, max_queue_size=1)

        receiver.queue.put_nowait(QueuesDownstreamRequest(RequestID="filler", WaitTimeout=1000))

        request = QueuesDownstreamRequest(RequestID="req-full", WaitTimeout=1000)
        result = receiver.send(request)

        assert result.IsError is True
        assert "full" in result.Error.lower()


# ==============================================================================
# SendWithoutResponse Tests
# ==============================================================================


class TestDownstreamReceiverSendWithoutResponse:
    """Tests for DownstreamReceiver.send_without_response()."""

    def test_send_without_response_enqueues(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        request = QueuesDownstreamRequest(RequestID="req-nr", WaitTimeout=1000)
        receiver.send_without_response(request)

        assert not receiver.queue.empty()
        queued = receiver.queue.get_nowait()
        assert queued.RequestID == "req-nr"

    def test_send_without_response_when_disconnected(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_transport.is_connected.return_value = False
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        request = QueuesDownstreamRequest(RequestID="req-nr", WaitTimeout=1000)
        with pytest.raises(ConnectionError, match="not connected"):
            receiver.send_without_response(request)

    def test_send_without_response_queue_full(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection, max_queue_size=1)

        receiver.queue.put_nowait(QueuesDownstreamRequest(RequestID="filler", WaitTimeout=1000))

        request = QueuesDownstreamRequest(RequestID="req-full", WaitTimeout=1000)
        with pytest.raises(KubeMQBufferFullError):
            receiver.send_without_response(request)


# ==============================================================================
# HandleDisconnection Tests
# ==============================================================================


class TestDownstreamReceiverHandleDisconnection:
    """Tests for DownstreamReceiver._handle_disconnection()."""

    def test_clears_pending_requests(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        container1, event1 = {}, threading.Event()
        container2, event2 = {}, threading.Event()
        receiver.response_tracking["req-a"] = (container1, event1)
        receiver.response_tracking["req-b"] = (container2, event2)

        receiver._handle_disconnection()

        assert receiver.response_tracking == {}
        assert receiver.allow_new_requests is False

        assert event1.is_set()
        assert event2.is_set()
        assert container1["response"].IsError is True
        assert "Disconnected" in container1["response"].Error
        assert container2["response"].IsError is True
        assert "Disconnected" in container2["response"].Error


# ==============================================================================
# RecreateChannel Tests
# ==============================================================================


class TestDownstreamReceiverRecreateChannel:
    """Tests for DownstreamReceiver._recreate_channel()."""

    def test_success(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)
        receiver.allow_new_requests = False

        new_stub = MagicMock()
        mock_transport.recreate_channel.return_value = new_stub

        result = receiver._recreate_channel()

        assert result is True
        assert receiver.clientStub is new_stub
        assert receiver.allow_new_requests is True

    def test_auto_reconnect_disabled(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_connection.disable_auto_reconnect = True
        mock_transport.recreate_channel.side_effect = ConnectionError("refused")
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        result = receiver._recreate_channel()

        assert result is False
        assert receiver.allow_new_requests is False

    def test_generic_failure(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_transport.recreate_channel.side_effect = Exception("unexpected")
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        result = receiver._recreate_channel()

        assert result is False


# ==============================================================================
# HandleError Tests
# ==============================================================================


class TestDownstreamReceiverHandleError:
    """Tests for DownstreamReceiver._handle_error()."""

    def test_grpc_error(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_transport.recreate_channel.return_value = MagicMock()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        error = FakeRpcError()
        with (
            patch.object(receiver, "_handle_disconnection") as mock_disc,
            patch.object(receiver, "_recreate_channel", return_value=True) as mock_recreate,
        ):
            result = receiver._handle_error(error, is_grpc_error=True)

        assert result is True
        mock_disc.assert_called_once()
        mock_recreate.assert_called_once()

    def test_generic_error(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        error = RuntimeError("something broke")
        with patch.object(receiver, "_handle_disconnection") as mock_disc:
            result = receiver._handle_error(error)

        assert result is True
        mock_disc.assert_called_once()

    def test_returns_false_when_unreconnectable(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_connection.disable_auto_reconnect = True
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        error = FakeRpcError()
        with (
            patch.object(receiver, "_handle_disconnection"),
            patch.object(receiver, "_recreate_channel", return_value=False),
        ):
            result = receiver._handle_error(error, is_grpc_error=True)

        assert result is False


# ==============================================================================
# Close Tests
# ==============================================================================


class TestDownstreamReceiverClose:
    """Tests for DownstreamReceiver.close()."""

    def test_close_sets_shutdown(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        receiver.close()

        assert receiver.shutdown_event.is_set()
        assert receiver.allow_new_requests is False


# ==============================================================================
# CloseByServer Tests (GAP-C3)
# ==============================================================================


class TestDownstreamReceiverCloseByServer:
    """GAP-C3: Tests for CloseByServer handling in _process_responses."""

    def test_close_by_server_triggers_disconnection(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        close_response = QueuesDownstreamResponse(
            RefRequestId="",
            RequestTypeData=QueuesDownstreamRequestType.CloseByServer,
            IsError=False,
        )

        with patch.object(receiver, "_handle_disconnection") as mock_disc:
            receiver._process_responses(iter([close_response]))
            mock_disc.assert_called_once()

    def test_close_by_server_notifies_pending_requests(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        container, event = {}, threading.Event()
        receiver.response_tracking["pending-req"] = (container, event)

        close_response = QueuesDownstreamResponse(
            RefRequestId="",
            RequestTypeData=QueuesDownstreamRequestType.CloseByServer,
            IsError=False,
        )

        receiver._process_responses(iter([close_response]))

        assert event.is_set()
        assert container["response"].IsError is True
        assert "Disconnected" in container["response"].Error
        assert receiver.allow_new_requests is False


# ==============================================================================
# Queue Utilization Warning Tests
# ==============================================================================


class TestDownstreamReceiverQueueUtilization:
    """Tests for receive queue utilization warning."""

    def test_send_queue_high_utilization_warning(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(
            mock_transport,
            mock_logger,
            mock_connection,
            max_queue_size=10,
            timeout_buffer=0.0,
        )

        for i in range(9):
            receiver.queue.put_nowait(
                QueuesDownstreamRequest(RequestID=f"filler-{i}", WaitTimeout=10)
            )

        request = QueuesDownstreamRequest(RequestID="req-warn", WaitTimeout=10)
        receiver.send(request)

        mock_logger.warning.assert_called()


# ==============================================================================
# SendWithoutResponse Not Accepting Tests
# ==============================================================================


class TestDownstreamReceiverSendWithoutResponseNotAccepting:
    """Tests for send_without_response when receiver is not accepting."""

    def test_send_without_response_when_not_accepting(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)
        receiver.allow_new_requests = False

        request = QueuesDownstreamRequest(RequestID="req-nr", WaitTimeout=1000)
        with pytest.raises(ConnectionError, match="not ready"):
            receiver.send_without_response(request)


# ==============================================================================
# GenerateRequests Tests
# ==============================================================================


class TestDownstreamReceiverGenerateRequests:
    """Tests for DownstreamReceiver._generate_requests()."""

    def test_sentinel_exits_generator(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        req = QueuesDownstreamRequest(RequestID="req-1", WaitTimeout=1000)
        receiver.queue.put(req)
        receiver.queue.put(None)

        results = list(receiver._generate_requests())

        assert len(results) == 1
        assert results[0].RequestID == "req-1"


# ==============================================================================
# HandleError Non-Channel Error Tests
# ==============================================================================


class TestDownstreamReceiverHandleErrorNonChannel:
    """Tests for _handle_error with non-channel, non-grpc error."""

    def test_non_channel_error_logs_and_continues(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        error = ValueError("bad data")
        with patch.object(receiver, "_handle_disconnection") as mock_disc:
            result = receiver._handle_error(error)

        assert result is True
        mock_disc.assert_called_once()
        error_calls = [str(c) for c in mock_logger.error.call_args_list]
        assert any("Non-channel error" in c for c in error_calls)


# ==============================================================================
# Close Tests (Extended)
# ==============================================================================


class TestDownstreamReceiverCloseExtended:
    """Extended tests for DownstreamReceiver.close()."""

    def test_close_sets_all_shutdown_state(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        receiver.close()

        assert receiver.shutdown_event.is_set()
        assert receiver.allow_new_requests is False
        sentinel = receiver.queue.get_nowait()
        assert sentinel is None


# ==============================================================================
# SendQueueStream Tests
# ==============================================================================


class TestDownstreamReceiverSendQueueStream:
    """Tests for DownstreamReceiver._send_queue_stream() error paths."""

    def test_grpc_error_path(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        stub = MagicMock()
        stub.QueuesDownstream.side_effect = FakeRpcError()
        mock_transport.kubemq_client.return_value = stub

        def handle_and_stop(error, is_grpc_error=False):
            receiver.shutdown_event.set()
            return True

        with patch.object(receiver, "_handle_error", side_effect=handle_and_stop) as mock_handle:
            receiver._send_queue_stream()

        mock_handle.assert_called_once()
        args, kwargs = mock_handle.call_args
        assert isinstance(args[0], grpc.RpcError)
        assert kwargs.get("is_grpc_error") is True

    def test_generic_exception_path(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        stub = MagicMock()
        stub.QueuesDownstream.side_effect = RuntimeError("boom")
        mock_transport.kubemq_client.return_value = stub

        def handle_and_stop(error, is_grpc_error=False):
            receiver.shutdown_event.set()
            return True

        with patch.object(receiver, "_handle_error", side_effect=handle_and_stop) as mock_handle:
            receiver._send_queue_stream()

        mock_handle.assert_called_once()
        args, kwargs = mock_handle.call_args
        assert isinstance(args[0], RuntimeError)
        assert kwargs.get("is_grpc_error", False) is False

    def test_exits_when_handle_error_returns_false(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        stub = MagicMock()
        stub.QueuesDownstream.side_effect = FakeRpcError()
        mock_transport.kubemq_client.return_value = stub

        with patch.object(receiver, "_handle_error", return_value=False) as mock_handle:
            receiver._send_queue_stream()

        mock_handle.assert_called_once()
        assert not receiver.shutdown_event.is_set()


# ==============================================================================
# Coverage Gap Tests
# ==============================================================================


class TestDownstreamReceiverProcessResponsesCoverage:
    """Cover lines 231-239: normal response matching in _process_responses."""

    def test_normal_response_resolves_tracked_request(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)
        receiver.allow_new_requests = False

        container, event = {}, threading.Event()
        receiver.response_tracking["req-match"] = (container, event)

        response = QueuesDownstreamResponse(RefRequestId="req-match", IsError=False)
        receiver._process_responses(iter([response]))

        assert event.is_set()
        assert container["response"] is response
        assert receiver.allow_new_requests is True

    def test_shutdown_breaks_response_loop(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)
        receiver.shutdown_event.set()

        container, event = {}, threading.Event()
        receiver.response_tracking["req-1"] = (container, event)

        response = QueuesDownstreamResponse(RefRequestId="req-1", IsError=False)
        receiver._process_responses(iter([response]))

        assert not event.is_set()

    def test_untracked_response_sets_allow_new_requests(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)
        receiver.allow_new_requests = False

        response = QueuesDownstreamResponse(RefRequestId="untracked", IsError=False)
        receiver._process_responses(iter([response]))

        assert receiver.allow_new_requests is True


class TestDownstreamReceiverRecreateChannelConnectionErrorEnabled:
    """Cover lines 260-261: ConnectionError with auto_reconnect enabled."""

    def test_connection_error_auto_reconnect_enabled(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        mock_connection.disable_auto_reconnect = False
        mock_transport.recreate_channel.side_effect = ConnectionError("refused")
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        result = receiver._recreate_channel()

        assert result is False
        error_calls = [str(c) for c in mock_logger.error.call_args_list]
        assert any("Connection error" in c for c in error_calls)


class TestDownstreamReceiverHandleErrorChannelError:
    """Cover lines 285-286: non-gRPC channel error triggers recreate."""

    def test_channel_error_triggers_recreate(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        error = Exception("channel closed unexpectedly")
        with (
            patch.object(receiver, "_handle_disconnection"),
            patch.object(receiver, "_recreate_channel", return_value=True) as mock_recreate,
        ):
            result = receiver._handle_error(error)

        assert result is True
        mock_recreate.assert_called_once()
        error_calls = [str(c) for c in mock_logger.error.call_args_list]
        assert any("Channel error" in c for c in error_calls)


class TestDownstreamReceiverSendQueueStreamGenericFalse:
    """Cover line 316: generic exception where _handle_error returns False."""

    def test_generic_exception_exits_thread(self):
        mock_transport, mock_logger, mock_connection = _make_mocks()
        receiver = _make_receiver(mock_transport, mock_logger, mock_connection)

        stub = MagicMock()
        stub.QueuesDownstream.side_effect = RuntimeError("boom")
        mock_transport.kubemq_client.return_value = stub

        with patch.object(receiver, "_handle_error", return_value=False):
            receiver._send_queue_stream()

        assert not receiver.shutdown_event.is_set()
