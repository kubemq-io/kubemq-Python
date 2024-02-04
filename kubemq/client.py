import logging
import threading
import asyncio
import grpc
from kubemq.config import Connection, TlsConfig, KeepAliveConfig
from kubemq.entities import *
from kubemq.transport import ServerInfo, Transport


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

    def send(self, message: [Event, EventStore, Command, Query, CommandResponse, QueryResponse]) -> [CommandResponse,
                                                                                                     QueryResponse,
                                                                                                     None]:
        if isinstance(message, Event):
            return self._send_event(message)
        if isinstance(message, EventStore):
            return self._send_event(message)
        if isinstance(message, Command):
            return self._send_request(message)
        if isinstance(message, CommandResponse):
            return self._send_response(message)
        if isinstance(message, Query):
            return self._send_request(message)
        if isinstance(message, QueryResponse):
            return self._send_response(message)
        return None

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

    def _send_response(self, response_to_send: [CommandResponse, QueryResponse]):
        try:
            response_to_send.validate()
            if isinstance(response_to_send, CommandResponse):
                self._transport.kubemq_client().SendResponse(
                    response_to_send.to_kubemq_command_response(self._connection.client_id))
            if isinstance(response_to_send, QueryResponse):
                self._transport.kubemq_client().SendResponse(
                    response_to_send.to_kubemq_query_response(self._connection.client_id))
        except ValueError as e:
            ex = ValidationError(str(e))
            self._logger.error(str(ex))
            raise ex
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def _send_request(self, request_to_send: [Command, Query]) -> [CommandResponse, QueryResponse]:
        try:
            request_to_send.validate()
            if isinstance(request_to_send, Command):
                response = self._transport.kubemq_client().SendRequest(
                    request_to_send.to_kubemq_command(self._connection.client_id))
                return CommandResponse().from_kubemq_command_response(response)
            if isinstance(request_to_send, Query):
                response = self._transport.kubemq_client().SendRequest(
                    request_to_send.to_kubemq_query(self._connection.client_id))
                return QueryResponse().from_kubemq_query_response(response)

        except ValueError as e:
            ex = ValidationError(str(e))
            self._logger.error(str(ex))
            raise ex
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def subscribe(self, subscription: [EventsSubscription, EventsStoreSubscription, CommandsSubscription,
                                       QueriesSubscription],
                  cancellation_token: CancellationToken):
        return self._subscribe(subscription, cancellation_token)

    def _subscribe(self, subscription: [EventsStoreSubscription, EventsSubscription, CommandsSubscription,
                                        QueriesSubscription],
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
                            if isinstance(subscription, EventsStoreSubscription):
                                response_stream = self._transport.kubemq_client().SubscribeToEvents(
                                    subscription.to_subscribe_request(self._connection.client_id))
                            if isinstance(subscription, EventsSubscription):
                                response_stream = self._transport.kubemq_client().SubscribeToEvents(
                                    subscription.to_subscribe_request(self._connection.client_id))
                            if isinstance(subscription, CommandsSubscription):
                                response_stream = self._transport.kubemq_client().SubscribeToRequests(
                                    subscription.to_subscribe_request(self._connection.client_id))
                            if isinstance(subscription, QueriesSubscription):
                                response_stream = self._transport.kubemq_client().SubscribeToRequests(
                                    subscription.to_subscribe_request(self._connection.client_id))
                            self._logger.debug(f"Subscribed to {subscription.channel}")
                            while not cancellation_token.is_cancelled:
                                message_receive = response_stream.next()
                                if isinstance(subscription, EventsStoreSubscription):
                                    subscription.raise_on_receive_message(
                                        EventStoreReceived().from_event(message_receive))
                                if isinstance(subscription, EventsSubscription):
                                    subscription.raise_on_receive_message(EventReceived().from_event(message_receive))
                                if isinstance(subscription, CommandsSubscription):
                                    subscription.raise_on_receive_message(
                                        CommandReceived().from_request(message_receive))
                                if isinstance(subscription, QueriesSubscription):
                                    subscription.raise_on_receive_message(QueryReceived().from_request(message_receive))
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
