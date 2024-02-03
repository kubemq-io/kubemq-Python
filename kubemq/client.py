import logging
import threading
import asyncio
import grpc

import kubemq
from kubemq.config import Connection, TlsConfig, KeepAliveConfig
from kubemq.exceptions import ValidationError, GRPCError
from kubemq.pubsub.events import *
from kubemq.pubsub.events_store import *
from kubemq.transport import ServerInfo, Transport


class CancellationToken:
    def __init__(self):
        self.is_cancelled = False

    def cancel(self):
        self.is_cancelled = True


class Client:
    def __init__(self, address: str = "",
                 client_id: str = "",
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
        self._connection: Connection = Connection(
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
        self._logger = logging.getLogger("KubeMQ")
        if log_level is not None:
            self._logger.setLevel(log_level)
        else:
            self._logger.setLevel(logging.CRITICAL + 1)
        try:
            self._connection.validate()
            self._transport: Transport = Transport(self._connection).initialize()
            self._logger.info(f"Client connected to {self._connection.address}")
        except ValueError as e:
            ex = ValidationError(e)
            self._logger.error(str(ex))
            raise ex
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        try:
            self._logger.debug(f"Client connecting to {self._connection.address}")
            self._transport: Transport = Transport(self._connection).initialize()
            self._logger.debug(f"Client connected to {self._connection.address}")
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def close(self):
        try:
            self._logger.debug(f"Client disconnecting from {self._connection.address}")
            self._transport.close()
            self._logger.debug(f"Client disconnected from {self._connection.address}")
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def ping(self) -> ServerInfo:
        try:
            self._logger.debug(f"Client pinging {self._connection.address}")
            return self._transport.ping()
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def _send_event(self, event_to_send: [Event, EventStore]):
        try:
            event_to_send.validate()
            self._transport.kubemq_client().SendEvent(event_to_send.to_kubemq_event(self._connection.client_id))
        except ValueError as e:
            ex = ValidationError(str(e))
            self._logger.error(str(ex))
            raise ex
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def send_event(self, event_to_send: Event) -> None:
        return self._send_event(event_to_send)

    def subscribe_to_events(self, subscription: EventsSubscription, cancellation_token: CancellationToken):
        return self._subscribe_to_events(subscription, cancellation_token)

    def send_event_store(self, event_to_send: EventStore):
        return self._send_event(event_to_send)

    def subscribe_to_events_store(self, subscription: EventsStoreSubscription,
                                  cancellation_token: CancellationToken):
        return self._subscribe_to_events(subscription, cancellation_token)

    def _subscribe_to_events(self, subscription: [EventsStoreSubscription, EventsSubscription],
                             cancellation_token: CancellationToken):
        try:
            subscription.validate()
            if cancellation_token is None:
                self._logger.error("A CancellationToken must be provided to subscribe to events store.")
                raise ValueError("A CancellationToken must be provided to subscribe to events store.")

            async def subscription_task():
                try:
                    while not cancellation_token.is_cancelled:
                        try:
                            response_stream = self._transport.kubemq_client().SubscribeToEvents(
                                subscription.to_subscribe_request(self._connection.client_id))
                            self._logger.debug(f"Subscribed to {subscription.channel}")
                            while not cancellation_token.is_cancelled:
                                event_receive = response_stream.next()
                                subscription.raise_on_receive_event(EventStoreReceived().from_event(event_receive))
                            if cancellation_token.is_cancelled:
                                self._logger.debug(f"Unsubscribed from {subscription.channel}")
                                break
                        except grpc._channel._MultiThreadedRendezvous as e:
                            subscription.raise_on_error(str(e))
                            self._logger.debug(
                                f"Connection Error on subscription to {subscription.channel} - {e.details()}, "
                                f"reconnecting in {self._connection.reconnect_interval_seconds} seconds...")
                            await asyncio.sleep(self._connection.reconnect_interval_seconds)
                            if cancellation_token.is_cancelled:
                                self._logger.debug(f"Unsubscribed from {subscription.channel}")
                                break
                        except Exception as e:
                            subscription.raise_on_error(str(e))
                            self._logger.debug(
                                f"Error on subscription to {subscription.channel} - {str(e)}, reconnecting in {self._connection.reconnect_interval_seconds} seconds...")
                            await asyncio.sleep(self._connection.reconnect_interval_seconds)
                            if cancellation_token.is_cancelled:
                                self._logger.debug(f"Unsubscribed from {subscription.channel}")
                                break
                        finally:
                            if response_stream:
                                response_stream.cancel()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    raise e

            loop = asyncio.new_event_loop()
            subscription_task_coroutine = subscription_task()
            subscription_thread = threading.Thread(target=lambda: loop.run_until_complete(subscription_task_coroutine),
                                                   daemon=True)
            subscription_thread.start()
        except ValueError as e:
            ex = ValidationError(str(e))
            self._logger.error(str(ex))
            raise ex
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex
#
# class AsyncClient:
#     def __init__(self, address: str = "",
#                  client_id: str = "",
#                  auth_token: str = "",
#                  tls: bool = False,
#                  tls_cert_file: str = "",
#                  tls_key_file: str = "",
#                  tls_ca_file: str = "",
#                  max_send_size: int = 0,
#                  max_receive_size: int = 0,
#                  disable_auto_reconnect: bool = False,
#                  reconnect_interval_seconds: int = 0,
#                  keep_alive: bool = False,
#                  ping_interval_in_seconds: int = 0,
#                  ping_timeout_in_seconds: int = 0) -> None:
#         self._connection: Connection = Connection(
#             address=address,
#             client_id=client_id,
#             auth_token=auth_token,
#             tls=TlsConfig(
#                 ca_file=tls_ca_file,
#                 cert_file=tls_cert_file,
#                 key_file=tls_key_file,
#                 enabled=tls,
#             ),
#             max_send_size=max_send_size,
#             max_receive_size=max_receive_size,
#             disable_auto_reconnect=disable_auto_reconnect,
#             reconnect_interval_seconds=reconnect_interval_seconds,
#             keep_alive=KeepAliveConfig(
#                 enabled=keep_alive,
#                 ping_interval_in_seconds=ping_interval_in_seconds,
#                 ping_timeout_in_seconds=ping_timeout_in_seconds
#             ))
#         try:
#             self._connection.validate()
#         except Exception as e:
#             raise ValidationError(e)
#         self._transport = AsyncTransport(self._connection)
#         self._transport.initialize()
#
#     async def connect(self) -> 'AsyncClient':
#         try:
#             await self._transport.initialize()
#         except Exception as e:
#             raise ConnectionError(e)
#         return self
#
#     async def close(self):
#         await self._transport.close()
#
#     async def ping(self) -> ServerInfo:
#         if not self._transport.is_connected():
#             raise ConnectionError("Not connected")
#         return await self._transport.ping()
#
#     async def __aenter__(self):
#         await self.connect()
#         return self
#
#     async def __aexit__(self, exc_type, exc_value, traceback):
#         await self.close()
