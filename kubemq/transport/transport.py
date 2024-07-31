import grpc
from grpc import Channel
from grpc._cython.cygrpc import ChannelCredentials
import kubemq.grpc.kubemq_pb2_grpc as kubemq_pb2_grpc
from typing import Sequence
from kubemq.transport.connection import Connection
from kubemq.transport.tls_config import TlsConfig
from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.interceptors import AuthInterceptorsAsync, AuthInterceptors
from kubemq.transport.server_info import ServerInfo
from kubemq.grpc import Empty


def _get_ssl_credentials(tls_config: TlsConfig) -> ChannelCredentials:
    certificate_chain = (
        _read_file(tls_config.cert_file) if tls_config.cert_file else None
    )
    private_key = _read_file(tls_config.key_file) if tls_config.key_file else None
    root_certificates = _read_file(tls_config.ca_file) if tls_config.ca_file else None
    return grpc.ssl_channel_credentials(
        root_certificates, private_key, certificate_chain
    )


def _read_file(file_path):
    with open(file_path, "rb") as f:
        return f.read()


def _get_call_options(connection: Connection) -> Sequence:
    options = [
        ("grpc.max_send_message_length", connection.max_send_size),
        ("grpc.max_receive_message_length", connection.max_receive_size),
    ]
    if (
        connection.keep_alive
        and isinstance(connection.keep_alive, KeepAliveConfig)
        and connection.keep_alive.enabled
    ):
        options.append(
            (
                "grpc.keepalive_time_ms",
                connection.keep_alive.ping_timeout_in_seconds * 1000,
            )
        )
        options.append(
            (
                "grpc.keepalive_timeout_ms",
                connection.keep_alive.ping_interval_in_seconds * 1000,
            )
        )
        options.append(("grpc.keepalive_permit_without_calls", 1))
        options.append(
            (
                "grpc.http2.min_time_between_pings_ms",
                connection.keep_alive.ping_timeout_in_seconds * 1000,
            )
        )
        options.append(
            (
                "grpc.http2.min_ping_interval_without_data_ms",
                connection.keep_alive.ping_interval_in_seconds * 1000,
            )
        )
    return options


class Transport:
    def __init__(self, connection: Connection) -> None:
        self._opts: Connection = connection.complete()
        self._channel: Channel = None
        self._client: kubemq_pb2_grpc.kubemqStub = None
        self._async_channel: Channel = None
        self._async_client: kubemq_pb2_grpc.kubemqStub = None
        self._is_connected: bool = False

    def initialize(self) -> "Transport":
        try:
            auth_interceptor = AuthInterceptors(self._opts.auth_token)
            interceptors = [auth_interceptor]
            credentials = (
                _get_ssl_credentials(self._opts.tls) if self._opts.tls.enabled else None
            )
            self._channel = (
                grpc.secure_channel(
                    self._opts.address,
                    credentials,
                    options=_get_call_options(self._opts),
                )
                if credentials
                else grpc.insecure_channel(
                    self._opts.address, options=_get_call_options(self._opts)
                )
            )
            self._channel = grpc.intercept_channel(self._channel, *interceptors)
            self._client = kubemq_pb2_grpc.kubemqStub(self._channel)
            self.ping()
            self._initialize_async()
            self._is_connected = True
        except Exception as ex:
            self._is_connected = False
            raise ex
        return self

    def _initialize_async(self) -> None:
        auth_interceptor_async: AuthInterceptorsAsync = AuthInterceptorsAsync(
            self._opts.auth_token
        )
        interceptors_async: Sequence[grpc.aio.ClientInterceptor] = [
            auth_interceptor_async
        ]
        if self._opts.tls.enabled:
            try:
                credentials: ChannelCredentials = _get_ssl_credentials(self._opts.tls)
                self._async_channel = grpc.aio.secure_channel(
                    self._opts.address,
                    credentials,
                    options=_get_call_options(self._opts),
                    interceptors=interceptors_async,
                )
            except Exception as e:
                raise e
        else:
            self._async_channel = grpc.aio.insecure_channel(
                self._opts.address,
                options=_get_call_options(self._opts),
                interceptors=interceptors_async,
            )

        self._async_client = kubemq_pb2_grpc.kubemqStub(self._channel)

    def ping(self) -> ServerInfo:
        response = self._client.Ping(Empty())
        return ServerInfo(
            host=response.Host,
            version=response.Version,
            server_start_time=response.ServerStartTime,
            server_up_time_seconds=response.ServerUpTimeSeconds,
        )

    def kubemq_client(self) -> kubemq_pb2_grpc.kubemqStub:
        return self._client

    def kubemq_async_client(self) -> kubemq_pb2_grpc.kubemqStub:
        return self._async_client

    def is_connected(self) -> bool:
        return self._is_connected

    def close(self) -> None:
        if self._is_connected and self._channel is not None:
            self._channel.close()
            self._is_connected = False
            self._channel = None
            self._client = None


class AsyncTransport:
    def __init__(self, connection: Connection) -> None:
        self._opts: Connection = connection.complete()
        self._channel: Channel = None
        self._client: kubemq_pb2_grpc.kubemqStub = None
        self._is_connected: bool = False

    async def initialize(self) -> "AsyncTransport":
        auth_interceptor_async: AuthInterceptorsAsync = AuthInterceptorsAsync(
            self._opts.auth_token
        )
        interceptors_async: Sequence[grpc.aio.ClientInterceptor] = [
            auth_interceptor_async
        ]

        if self._opts.tls.enabled:
            try:
                credentials: ChannelCredentials = _get_ssl_credentials(self._opts.tls)
                self._channel = grpc.aio.secure_channel(
                    self._opts.address,
                    credentials,
                    options=_get_call_options(self._opts),
                    interceptors=interceptors_async,
                )
            except Exception as e:
                raise e
        else:
            self._channel = grpc.aio.insecure_channel(
                self._opts.address,
                options=_get_call_options(self._opts),
                interceptors=interceptors_async,
            )

        self._client = kubemq_pb2_grpc.kubemqStub(self._channel)
        try:
            await self.ping()
            self._is_connected = True
        except Exception as ex:
            self._is_connected = False
            raise ex
        return self

    async def ping(self) -> ServerInfo:
        response = await self._client.Ping(Empty())
        return ServerInfo(
            host=response.Host,
            version=response.Version,
            server_start_time=response.ServerStartTime,
            server_up_time_seconds=response.ServerUpTimeSeconds,
        )

    def kubemq_client(self) -> kubemq_pb2_grpc.kubemqStub:
        return self._client

    def is_connected(self) -> bool:
        return self._is_connected

    async def close(self) -> None:
        if self._is_connected and self._channel is not None:
            await self._channel.close()
            self._is_connected = False
            self._channel = None
            self._client = None
