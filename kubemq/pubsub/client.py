import logging
import threading
import asyncio
import time
import uuid
import grpc
import socket
from typing import List
from kubemq.transport import *
from kubemq.grpc import (Event,Result,Request,Response)
from kubemq.pubsub import *
from kubemq.common.exceptions import *
from kubemq.common.helpers import *
from kubemq.common.requests import *
from kubemq.common.subscribe_type import *
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
            self.logger.info(f"Client connected to {self.connection.address}")
            self.shutdown_event: threading.Event = threading.Event()
            self.event_sender = None
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
    def send_events_message(self, message: EventMessage) :
        message.validate()
        if self.event_sender is None:
            self.event_sender = EventSender(self.transport, self.shutdown_event, self.logger, self.connection)
        self.event_sender.send(message.encode(self.connection.client_id))
    def send_events_store_message(self, message: EventStoreMessage) -> EventSendResult:
        message.validate()
        if self.event_sender is None:
            self.event_sender = EventSender(self.transport, self.shutdown_event, self.logger, self.connection)
        result = self.event_sender.send(message.encode(self.connection.client_id))
        if result is not None:
            return EventSendResult().decode(result)

    def delete_events_channel(self, channel: str):
        return delete_channel_request(self.transport, self.connection.client_id, channel, "events")
    def delete_events_store_channel(self, channel: str):
        return delete_channel_request(self.transport, self.connection.client_id, channel, "events_store")
    def create_events_channel(self, channel: str)->[bool,None]:
        return create_channel_request(self.transport, self.connection.client_id, channel, "events")
    def create_events_store_channel(self, channel: str):
        return create_channel_request(self.transport, self.connection.client_id, channel, "events_store")
    def list_events_channels(self, channel_search: str = "") -> List[Channel]:
        return self._list_channels("events", channel_search)
    def list_events_store_channels(self, channel_search: str = "") -> List[Channel]:
        return self._list_channels("events_store", channel_search)
    def _list_channels(self, channel_type: str, channel_search)->List[Channel]:
        try:
            self.logger.debug(f"Client listing {channel_type} channels")
            request = Request(
                RequestID=str(uuid.uuid4()),
                RequestTypeData=2,
                Metadata="list-channels",
                Channel=requests_channel,
                ClientID=self.connection.client_id,
                Tags={"channel_type": channel_type, "channel_search": channel_search},
                Timeout=10 * 1000
            )
            response = self.transport.kubemq_client().SendRequest(request)
            if response:
                if response.Executed:
                    self.logger.debug(f"Client listed {channel_type} channels")
                    return decode_channel_list(response.Body)
                else:
                    self.logger.error(f"Client failed to list {channel_type} channels, error: {response.Error}")
                    raise ListChannelsError(response.Error)
        except grpc.RpcError as e:
            raise GRPCError(decode_grpc_error(e))
    def subscribe_to_events(self, subscription: EventsSubscription, cancel: CancellationToken = None):
        self._subscribe(subscription, cancel)

    def subscribe_to_events_store(self, subscription: EventsStoreSubscription, cancel: CancellationToken = None):
        self._subscribe(subscription, cancel)
    def _subscribe(self,
                  subscription: [EventsSubscription,EventsStoreSubscription],
                  cancel: [CancellationToken, None]):
        if cancel is None:
            cancel = CancellationToken()
        cancel_token_event = cancel.event
        subscription.validate()
        args = ()
        if isinstance(subscription, EventsStoreSubscription):
            args = (lambda: self.transport.kubemq_client().SubscribeToEvents(
                subscription.encode(self.connection.client_id)),
                    lambda message: subscription.raise_on_receive_message(
                        EventStoreMessageReceived().decode(message)),
                    lambda error: subscription.raise_on_error(error),
                    cancel_token_event)
        if isinstance(subscription, EventsSubscription):
            args = (lambda: self.transport.kubemq_client().SubscribeToEvents(
                subscription.encode(self.connection.client_id)),
                    lambda message: subscription.raise_on_receive_message(
                        EventMessageReceived().decode(message)),
                    lambda error: subscription.raise_on_error(error),
                    cancel_token_event)
        threading.Thread(
            target=self._subscribe_task,
            args=args,
            daemon=True).start()

    def _subscribe_task(self, stream_callable, decode_callable, error_callable, cancel_token: threading.Event):
        while not cancel_token.is_set() and not self.shutdown_event.is_set():
            try:
                response = stream_callable()
                for message in response:
                    if cancel_token.is_set():
                        break
                    decode_callable(message)
            except grpc.RpcError as e:
                error_callable(decode_grpc_error(e))
                time.sleep(self.connection.reconnect_interval_seconds)
                continue
            except Exception as e:
                error_callable(decode_grpc_error(e))
                time.sleep(self.connection.reconnect_interval_seconds)
                continue
