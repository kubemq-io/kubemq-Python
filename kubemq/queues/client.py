import logging
import threading
import asyncio
import time
import uuid
import grpc
import socket
from typing import List
from kubemq.transport import *
from kubemq.queues.queues_message import *
from kubemq.queues import *
from kubemq.common.exceptions import *
from kubemq.common.helpers import *
from kubemq.common.requests import *
from kubemq.common.cancellation_token import *

class Client:
    def __init__(self, address: str = "",
                 client_id: str = socket.gethostname(),
                 auth_token: str = "",
                 tls: bool = False,
                 tls_cert_file: str = "",
                 tls_key_file: str = "",
                 tls_ca_file: str = "",
                 max_send_size: int = 0,
                 max_receive_size: int = 0,
                 disable_auto_reconnect: bool = False,
                 reconnect_interval_seconds: int = 0,
                 keep_alive: bool = False,
                 ping_interval_in_seconds: int = 0,
                 ping_timeout_in_seconds: int = 0,
                 log_level: int = None) -> None:
        self.connection: Connection = Connection(
            address=address,
            client_id=client_id,
            auth_token=auth_token,
            tls=TlsConfig(
                ca_file=tls_ca_file,
                cert_file=tls_cert_file,
                key_file=tls_key_file,
                enabled=tls,
            ),
            max_send_size=max_send_size,
            max_receive_size=max_receive_size,
            disable_auto_reconnect=disable_auto_reconnect,
            reconnect_interval_seconds=reconnect_interval_seconds,
            keep_alive=KeepAliveConfig(
                enabled=keep_alive,
                ping_interval_in_seconds=ping_interval_in_seconds,
                ping_timeout_in_seconds=ping_timeout_in_seconds
            ),
            log_level=log_level)
        self.logger = logging.getLogger("KubeMQ")
        if log_level is not None:
            self.logger.setLevel(log_level)
        else:
            self.logger.setLevel(logging.CRITICAL + 1)
        try:
            self.connection.validate()
            self.transport: Transport = Transport(self.connection).initialize()
            self.shutdown_event: threading.Event = threading.Event()
            self.upstream_sender = None
            self.downstream_receiver = None
        except ValueError as e:
            ex = ValidationError(e)
            self.logger.error(str(ex))
            raise ex
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        try:
            self.logger.debug(f"Client connecting to {self.connection.address}")
            self.transport: Transport = Transport(self.connection).initialize()
            self.logger.debug(f"Client connected to {self.connection.address}")
        except Exception as e:
            ex =GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def close(self):
        try:
            self.shutdown_event.set()
            self.logger.debug(f"Client disconnecting from {self.connection.address}")
            self.transport.close()
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def ping(self) -> ServerInfo:
        try:
            self.logger.debug(f"Client pinging {self.connection.address}")
            return self.transport.ping()
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def send_queues_message(self, message: QueueMessage) -> QueueSendResult:
        message.validate()
        if self.upstream_sender is None:
            self.upstream_sender = UpstreamSender(self.transport, self.shutdown_event, self.logger,self.connection)

        return self.upstream_sender.send(message)
    def create_queues_channel(self, channel: str)->[bool,None]:
        return create_channel_request(self.transport, self.connection.client_id, channel, "queues")

    def delete_queues_channel(self, channel: str)->[bool,None]:
        return delete_channel_request(self.transport, self.connection.client_id, channel, "queues")

    def list_queues_channels(self, channel_search)->List[QueuesChannel]:
       return list_queues_channels(self.transport, self.connection.client_id, channel_search)

    def receive_queues_messages(self, channel: str = None,
             max_messages: int = 1,
             wait_timeout_in_seconds: int = 60,
             auto_ack: bool = False,
             ) -> QueuesPollResponse:
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1:
            raise ValueError("poll_max_messages must be greater than 0.")
        if wait_timeout_in_seconds < 1:
            raise ValueError("poll_wait_timeout_in_seconds must be greater than 0.")

        if self.downstream_receiver is None:
            self.downstream_receiver = (
                DownstreamReceiver(self.transport,
                                         self.shutdown_event,
                                         self.logger,
                                         self.connection))

        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.connection.client_id
        request.Channel = channel
        request.MaxItems = max_messages
        request.WaitTimeout = wait_timeout_in_seconds * 1000
        request.AutoAck = auto_ack
        request.RequestTypeData = QueuesDownstreamRequestType.Get
        kubemq_response: QueuesDownstreamResponse = self.downstream_receiver.send(request)
        response = QueuesPollResponse().decode(
            response=kubemq_response,
            receiver_client_id=self.connection.client_id,
            response_handler=self.downstream_receiver.send_without_response)

        return response