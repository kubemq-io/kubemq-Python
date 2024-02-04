# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from kubemq.grpc import kubemq_pb2 as kubemq_dot_grpc_dot_kubemq__pb2


class kubemqStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SendEvent = channel.unary_unary(
                '/kubemq.kubemq/SendEvent',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Event.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Result.FromString,
                )
        self.SendEventsStream = channel.stream_stream(
                '/kubemq.kubemq/SendEventsStream',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Event.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Result.FromString,
                )
        self.SubscribeToEvents = channel.unary_stream(
                '/kubemq.kubemq/SubscribeToEvents',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Subscribe.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.EventReceive.FromString,
                )
        self.SubscribeToRequests = channel.unary_stream(
                '/kubemq.kubemq/SubscribeToRequests',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Subscribe.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Request.FromString,
                )
        self.SendRequest = channel.unary_unary(
                '/kubemq.kubemq/SendRequest',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Request.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Response.FromString,
                )
        self.SendResponse = channel.unary_unary(
                '/kubemq.kubemq/SendResponse',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Response.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Empty.FromString,
                )
        self.SendQueueMessage = channel.unary_unary(
                '/kubemq.kubemq/SendQueueMessage',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.QueueMessage.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.SendQueueMessageResult.FromString,
                )
        self.SendQueueMessagesBatch = channel.unary_unary(
                '/kubemq.kubemq/SendQueueMessagesBatch',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.QueueMessagesBatchRequest.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.QueueMessagesBatchResponse.FromString,
                )
        self.ReceiveQueueMessages = channel.unary_unary(
                '/kubemq.kubemq/ReceiveQueueMessages',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.ReceiveQueueMessagesRequest.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.ReceiveQueueMessagesResponse.FromString,
                )
        self.StreamQueueMessage = channel.stream_stream(
                '/kubemq.kubemq/StreamQueueMessage',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.StreamQueueMessagesRequest.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.StreamQueueMessagesResponse.FromString,
                )
        self.AckAllQueueMessages = channel.unary_unary(
                '/kubemq.kubemq/AckAllQueueMessages',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.AckAllQueueMessagesRequest.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.AckAllQueueMessagesResponse.FromString,
                )
        self.Ping = channel.unary_unary(
                '/kubemq.kubemq/Ping',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Empty.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.PingResult.FromString,
                )
        self.QueuesDownstream = channel.stream_stream(
                '/kubemq.kubemq/QueuesDownstream',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesDownstreamRequest.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesDownstreamResponse.FromString,
                )
        self.QueuesUpstream = channel.stream_stream(
                '/kubemq.kubemq/QueuesUpstream',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesUpstreamRequest.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesUpstreamResponse.FromString,
                )
        self.QueuesInfo = channel.unary_unary(
                '/kubemq.kubemq/QueuesInfo',
                request_serializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesInfoRequest.SerializeToString,
                response_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesInfoResponse.FromString,
                )


class kubemqServicer(object):
    """Missing associated documentation comment in .proto file."""

    def SendEvent(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SendEventsStream(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SubscribeToEvents(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SubscribeToRequests(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SendRequest(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SendResponse(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SendQueueMessage(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SendQueueMessagesBatch(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ReceiveQueueMessages(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def StreamQueueMessage(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AckAllQueueMessages(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Ping(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def QueuesDownstream(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def QueuesUpstream(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def QueuesInfo(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_kubemqServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'SendEvent': grpc.unary_unary_rpc_method_handler(
                    servicer.SendEvent,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Event.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Result.SerializeToString,
            ),
            'SendEventsStream': grpc.stream_stream_rpc_method_handler(
                    servicer.SendEventsStream,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Event.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Result.SerializeToString,
            ),
            'SubscribeToEvents': grpc.unary_stream_rpc_method_handler(
                    servicer.SubscribeToEvents,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Subscribe.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.EventReceive.SerializeToString,
            ),
            'SubscribeToRequests': grpc.unary_stream_rpc_method_handler(
                    servicer.SubscribeToRequests,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Subscribe.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Request.SerializeToString,
            ),
            'SendRequest': grpc.unary_unary_rpc_method_handler(
                    servicer.SendRequest,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Request.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Response.SerializeToString,
            ),
            'SendResponse': grpc.unary_unary_rpc_method_handler(
                    servicer.SendResponse,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Response.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.Empty.SerializeToString,
            ),
            'SendQueueMessage': grpc.unary_unary_rpc_method_handler(
                    servicer.SendQueueMessage,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.QueueMessage.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.SendQueueMessageResult.SerializeToString,
            ),
            'SendQueueMessagesBatch': grpc.unary_unary_rpc_method_handler(
                    servicer.SendQueueMessagesBatch,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.QueueMessagesBatchRequest.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.QueueMessagesBatchResponse.SerializeToString,
            ),
            'ReceiveQueueMessages': grpc.unary_unary_rpc_method_handler(
                    servicer.ReceiveQueueMessages,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.ReceiveQueueMessagesRequest.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.ReceiveQueueMessagesResponse.SerializeToString,
            ),
            'StreamQueueMessage': grpc.stream_stream_rpc_method_handler(
                    servicer.StreamQueueMessage,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.StreamQueueMessagesRequest.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.StreamQueueMessagesResponse.SerializeToString,
            ),
            'AckAllQueueMessages': grpc.unary_unary_rpc_method_handler(
                    servicer.AckAllQueueMessages,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.AckAllQueueMessagesRequest.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.AckAllQueueMessagesResponse.SerializeToString,
            ),
            'Ping': grpc.unary_unary_rpc_method_handler(
                    servicer.Ping,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.Empty.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.PingResult.SerializeToString,
            ),
            'QueuesDownstream': grpc.stream_stream_rpc_method_handler(
                    servicer.QueuesDownstream,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesDownstreamRequest.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesDownstreamResponse.SerializeToString,
            ),
            'QueuesUpstream': grpc.stream_stream_rpc_method_handler(
                    servicer.QueuesUpstream,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesUpstreamRequest.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesUpstreamResponse.SerializeToString,
            ),
            'QueuesInfo': grpc.unary_unary_rpc_method_handler(
                    servicer.QueuesInfo,
                    request_deserializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesInfoRequest.FromString,
                    response_serializer=kubemq_dot_grpc_dot_kubemq__pb2.QueuesInfoResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'kubemq.kubemq', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class kubemq(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def SendEvent(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/kubemq.kubemq/SendEvent',
            kubemq_dot_grpc_dot_kubemq__pb2.Event.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.Result.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SendEventsStream(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/kubemq.kubemq/SendEventsStream',
            kubemq_dot_grpc_dot_kubemq__pb2.Event.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.Result.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SubscribeToEvents(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/kubemq.kubemq/SubscribeToEvents',
            kubemq_dot_grpc_dot_kubemq__pb2.Subscribe.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.EventReceive.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SubscribeToRequests(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/kubemq.kubemq/SubscribeToRequests',
            kubemq_dot_grpc_dot_kubemq__pb2.Subscribe.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.Request.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SendRequest(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/kubemq.kubemq/SendRequest',
            kubemq_dot_grpc_dot_kubemq__pb2.Request.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SendResponse(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/kubemq.kubemq/SendResponse',
            kubemq_dot_grpc_dot_kubemq__pb2.Response.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SendQueueMessage(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/kubemq.kubemq/SendQueueMessage',
            kubemq_dot_grpc_dot_kubemq__pb2.QueueMessage.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.SendQueueMessageResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SendQueueMessagesBatch(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/kubemq.kubemq/SendQueueMessagesBatch',
            kubemq_dot_grpc_dot_kubemq__pb2.QueueMessagesBatchRequest.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.QueueMessagesBatchResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ReceiveQueueMessages(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/kubemq.kubemq/ReceiveQueueMessages',
            kubemq_dot_grpc_dot_kubemq__pb2.ReceiveQueueMessagesRequest.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.ReceiveQueueMessagesResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def StreamQueueMessage(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/kubemq.kubemq/StreamQueueMessage',
            kubemq_dot_grpc_dot_kubemq__pb2.StreamQueueMessagesRequest.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.StreamQueueMessagesResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def AckAllQueueMessages(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/kubemq.kubemq/AckAllQueueMessages',
            kubemq_dot_grpc_dot_kubemq__pb2.AckAllQueueMessagesRequest.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.AckAllQueueMessagesResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Ping(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/kubemq.kubemq/Ping',
            kubemq_dot_grpc_dot_kubemq__pb2.Empty.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.PingResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def QueuesDownstream(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/kubemq.kubemq/QueuesDownstream',
            kubemq_dot_grpc_dot_kubemq__pb2.QueuesDownstreamRequest.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.QueuesDownstreamResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def QueuesUpstream(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/kubemq.kubemq/QueuesUpstream',
            kubemq_dot_grpc_dot_kubemq__pb2.QueuesUpstreamRequest.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.QueuesUpstreamResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def QueuesInfo(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/kubemq.kubemq/QueuesInfo',
            kubemq_dot_grpc_dot_kubemq__pb2.QueuesInfoRequest.SerializeToString,
            kubemq_dot_grpc_dot_kubemq__pb2.QueuesInfoResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
