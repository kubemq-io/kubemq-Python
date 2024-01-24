
class Response:
    """The response that receiving from KubeMQ after sending a request."""

    def __init__(self, request):
        self.client_id = ""
        self.request_id = ""
        self.reply_channel = ""
        self.metadata = ""
        self.body = None
        self.cache_hit = False
        self.timestamp = datetime.now()
        self.executed = False
        self.error = ""
        self.tags = {}
        if isinstance(request, RequestReceive):
            self.request_id = request.request_id
            """Represents a Response identifier."""
            self.reply_channel = request.reply_channel
            """Channel name for the Response. Set and used internally by KubeMQ server."""

        elif isinstance(request, InnerResponse):

            self.client_id = request.ClientID or ""
            """Represents the sender ID that the Response will be send under."""

            self.request_id = request.RequestID
            """Represents a Response identifier."""

            self.reply_channel = request.ReplyChannel
            """Channel name for the Response. Set and used internally by KubeMQ server."""

            self.metadata = request.Metadata or ""
            """Represents text as str."""

            self.body = request.Body
            """Represents The content of the Response."""

            self.cache_hit = request.CacheHit
            """Represents if the response was received from Cache."""

            self.timestamp = datetime.fromtimestamp(request.Timestamp)
            """Represents if the response Time."""

            self.executed = request.Executed
            """Represents if the response was executed."""

            self.error = request.Error
            """Error message"""

            self.tags = request.Tags
            """Represents key value pairs that help distinguish the message"""

        else:
            raise Exception("Unknown type" + str(type(request)))

    def convert(self):
        return InnerResponse(
            ClientID=self.client_id or "",
            RequestID=self.request_id,
            ReplyChannel=self.reply_channel,
            Metadata=self.metadata or "",
            Body=self.body,
            CacheHit=self.cache_hit,
            Timestamp=int((self.timestamp - epoch).total_seconds()),
            Executed=self.executed,
            Error=self.error
        )
