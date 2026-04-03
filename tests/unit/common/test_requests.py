"""Unit tests for kubemq.common.requests module."""

from __future__ import annotations

import unittest.mock
from unittest.mock import MagicMock, patch

import grpc
import pytest

from kubemq.common.requests import (
    create_channel_request,
    delete_channel_request,
    list_cq_channels,
    list_pubsub_channels,
    list_queues_channels,
)
from kubemq.core.exceptions import KubeMQChannelError, KubeMQError


class FakeRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "server unavailable"


def _make_transport(response=None, side_effect=None):
    transport = MagicMock()
    client = MagicMock()
    if side_effect:
        client.SendRequest.side_effect = side_effect
    else:
        client.SendRequest.return_value = response
    transport.kubemq_client.return_value = client
    return transport


def _make_response(executed=True, error="", body=b""):
    resp = MagicMock()
    resp.Executed = executed
    resp.Error = error
    resp.Body = body
    return resp


class TestCreateChannelRequest:
    def test_success_returns_true(self):
        transport = _make_transport(response=_make_response(executed=True))
        result = create_channel_request(transport, "client-1", "my-channel", "queues")
        assert result is True

    def test_executed_false_raises_channel_error(self):
        transport = _make_transport(response=_make_response(executed=False, error="channel exists"))
        with pytest.raises(KubeMQChannelError):
            create_channel_request(transport, "client-1", "my-channel", "queues")

    def test_grpc_error_raises_kubemq_error(self):
        transport = _make_transport(side_effect=FakeRpcError())
        with pytest.raises(KubeMQError):
            create_channel_request(transport, "client-1", "my-channel", "queues")

    def test_none_response_returns_false(self):
        transport = _make_transport(response=None)
        result = create_channel_request(transport, "client-1", "my-channel", "queues")
        assert result is False


class TestDeleteChannelRequest:
    def test_success_returns_true(self):
        transport = _make_transport(response=_make_response(executed=True))
        result = delete_channel_request(transport, "client-1", "my-channel", "queues")
        assert result is True

    def test_executed_false_raises_channel_error(self):
        transport = _make_transport(response=_make_response(executed=False, error="not found"))
        with pytest.raises(KubeMQChannelError):
            delete_channel_request(transport, "client-1", "my-channel", "queues")

    def test_grpc_error_raises_kubemq_error(self):
        transport = _make_transport(side_effect=FakeRpcError())
        with pytest.raises(KubeMQError):
            delete_channel_request(transport, "client-1", "my-channel", "queues")

    def test_none_response_returns_false(self):
        transport = _make_transport(response=None)
        result = delete_channel_request(transport, "client-1", "my-channel", "queues")
        assert result is False


class TestListQueuesChannels:
    @patch("kubemq.common.requests.decode_queues_channel_list")
    def test_success_returns_decoded_channels(self, mock_decode):
        sentinel = [MagicMock()]
        mock_decode.return_value = sentinel
        transport = _make_transport(response=_make_response(executed=True, body=b"some-body"))
        result = list_queues_channels(transport, "client-1", "*")
        assert result is sentinel
        mock_decode.assert_called_once_with(b"some-body")

    def test_none_response_returns_empty_list(self):
        transport = _make_transport(response=None)
        result = list_queues_channels(transport, "client-1", "*")
        assert result == []

    def test_executed_false_raises_channel_error(self):
        transport = _make_transport(response=_make_response(executed=False, error="access denied"))
        with pytest.raises(KubeMQChannelError):
            list_queues_channels(transport, "client-1", "*")

    def test_grpc_error_raises_kubemq_error(self):
        transport = _make_transport(side_effect=FakeRpcError())
        with pytest.raises(KubeMQError):
            list_queues_channels(transport, "client-1", "*")


class TestListPubsubChannels:
    @patch("kubemq.common.requests.decode_pub_sub_channel_list")
    def test_success_returns_decoded_channels(self, mock_decode):
        sentinel = [MagicMock()]
        mock_decode.return_value = sentinel
        transport = _make_transport(response=_make_response(executed=True, body=b"body-data"))
        result = list_pubsub_channels(transport, "client-1", "events", "*")
        assert result is sentinel
        mock_decode.assert_called_once_with(b"body-data")

    def test_none_response_returns_empty_list(self):
        transport = _make_transport(response=None)
        result = list_pubsub_channels(transport, "client-1", "events", "*")
        assert result == []

    def test_executed_false_raises_channel_error(self):
        transport = _make_transport(response=_make_response(executed=False, error="bad request"))
        with pytest.raises(KubeMQChannelError):
            list_pubsub_channels(transport, "client-1", "events", "*")

    def test_grpc_error_raises_kubemq_error(self):
        transport = _make_transport(side_effect=FakeRpcError())
        with pytest.raises(KubeMQError):
            list_pubsub_channels(transport, "client-1", "events", "*")


class TestListCQChannels:
    @patch("kubemq.common.requests.decode_cq_channel_list")
    def test_success_returns_decoded_channels(self, mock_decode):
        sentinel = [MagicMock()]
        mock_decode.return_value = sentinel
        transport = _make_transport(response=_make_response(executed=True, body=b"cq-body"))
        result = list_cq_channels(transport, "client-1", "commands", "*")
        assert result is sentinel
        mock_decode.assert_called_once_with(b"cq-body")

    def test_none_response_returns_empty_list(self):
        transport = _make_transport(response=None)
        result = list_cq_channels(transport, "client-1", "commands", "*")
        assert result == []

    def test_executed_false_raises_channel_error(self):
        transport = _make_transport(response=_make_response(executed=False, error="timeout"))
        with pytest.raises(KubeMQChannelError):
            list_cq_channels(transport, "client-1", "commands", "*")

    def test_grpc_error_raises_kubemq_error(self):
        transport = _make_transport(side_effect=FakeRpcError())
        with pytest.raises(KubeMQError):
            list_cq_channels(transport, "client-1", "commands", "*")


class TestListChannelsRetry:
    """Tests for list channels retry logic."""

    def test_list_channels_retries_on_snapshot_not_ready(self):
        snapshot_resp = _make_response(executed=False, error="cluster snapshot not ready yet")
        success_resp = _make_response(executed=True, body=b"[]")
        transport = MagicMock()
        transport.kubemq_client.return_value.SendRequest.side_effect = [
            snapshot_resp,
            success_resp,
        ]

        with unittest.mock.patch("kubemq.common.requests.time.sleep"):
            result = list_queues_channels(transport, "client-1", "")

        assert result == []
        assert transport.kubemq_client.return_value.SendRequest.call_count == 2

    def test_list_channels_retries_on_timeout(self):
        class DeadlineExceeded(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.DEADLINE_EXCEEDED

            def details(self):
                return "Deadline Exceeded"

        success_resp = _make_response(executed=True, body=b"[]")
        transport = MagicMock()
        transport.kubemq_client.return_value.SendRequest.side_effect = [
            DeadlineExceeded(),
            success_resp,
        ]

        with unittest.mock.patch("kubemq.common.requests.time.sleep"):
            result = list_queues_channels(transport, "client-1", "")

        assert result == []
        assert transport.kubemq_client.return_value.SendRequest.call_count == 2

    def test_list_channels_succeeds_on_retry(self):
        fail_resp = _make_response(executed=False, error="cluster snapshot not ready yet")
        success_resp = _make_response(executed=True, body=b"[]")
        transport = MagicMock()
        transport.kubemq_client.return_value.SendRequest.side_effect = [
            fail_resp,
            fail_resp,
            success_resp,
        ]

        with unittest.mock.patch("kubemq.common.requests.time.sleep"):
            result = list_queues_channels(transport, "client-1", "")

        assert result == []
        assert transport.kubemq_client.return_value.SendRequest.call_count == 3


class TestListChannelsRetryGrpcRetryable:
    """Test _list_channels_with_retry: retryable gRPC error on first attempt, success on second."""

    def test_grpc_deadline_retry_then_success(self):
        class DeadlineExceeded(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.DEADLINE_EXCEEDED

            def details(self):
                return "Deadline Exceeded"

        success_resp = _make_response(executed=True, body=b"decoded")
        transport = MagicMock()
        transport.kubemq_client.return_value.SendRequest.side_effect = [
            DeadlineExceeded(),
            success_resp,
        ]

        decode_fn = MagicMock(return_value=["channel-a"])

        from kubemq.common.requests import _list_channels_with_retry

        with unittest.mock.patch("kubemq.common.requests.time.sleep"):
            result = _list_channels_with_retry(transport, "client-1", "queues", "", decode_fn)

        assert result == ["channel-a"]
        decode_fn.assert_called_once_with(b"decoded")
        assert transport.kubemq_client.return_value.SendRequest.call_count == 2


# ==============================================================================
# Coverage Gap Tests
# ==============================================================================


class TestListChannelsRetryExhausted:
    """Cover loop exhaustion paths in _list_channels_with_retry."""

    def test_returns_empty_when_max_retries_is_zero(self):
        from kubemq.common.requests import _list_channels_with_retry

        transport = MagicMock()
        decode_fn = MagicMock()

        with patch("kubemq.common.requests._LIST_MAX_RETRIES", 0):
            result = _list_channels_with_retry(transport, "client", "queues", "", decode_fn)

        assert result == []
        decode_fn.assert_not_called()
        transport.kubemq_client.assert_not_called()
