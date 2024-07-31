import grpc
from typing import Callable, Any
from grpc import ClientCallDetails


class AuthInterceptors(
    grpc.UnaryUnaryClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    def __init__(self, auth_token: str):
        self.auth_token = auth_token

    def _intercept_call(
        self,
        continuation: Callable[[ClientCallDetails, Any], Any],
        client_call_details: ClientCallDetails,
        request_or_iterator: Any,
    ) -> Any:
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)

        if self.auth_token and self.auth_token.strip():
            metadata.append(("authorization", f"{self.auth_token}"))

        new_client_call_details = client_call_details._replace(metadata=metadata)
        response = continuation(new_client_call_details, request_or_iterator)
        return response

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        return self._intercept_call(continuation, client_call_details, request_iterator)

    def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        return self._intercept_call(continuation, client_call_details, request_iterator)


class AuthInterceptorsAsync(
    grpc.aio.UnaryUnaryClientInterceptor,
    grpc.aio.StreamStreamClientInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
    grpc.aio.StreamUnaryClientInterceptor,
):
    def __init__(self, auth_token: str):
        self.auth_token = auth_token

    async def _intercept_call(
        self,
        continuation: Callable[[ClientCallDetails, Any], Any],
        client_call_details: ClientCallDetails,
        request_or_iterator: Any,
    ) -> Any:
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)

        if self.auth_token and self.auth_token.strip():
            metadata.append(("authorization", f"{self.auth_token}"))

        new_client_call_details = client_call_details._replace(metadata=metadata)
        response = await continuation(new_client_call_details, request_or_iterator)
        return response

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        return await self._intercept_call(continuation, client_call_details, request)

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        return await self._intercept_call(continuation, client_call_details, request)

    async def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        return await self._intercept_call(
            continuation, client_call_details, request_iterator
        )

    async def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        return await self._intercept_call(
            continuation, client_call_details, request_iterator
        )
