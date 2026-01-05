from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import (
    ClassVar as _ClassVar,
)

from google.protobuf import descriptor as _descriptor, message as _message
from google.protobuf.internal import (
    containers as _containers,
    enum_type_wrapper as _enum_type_wrapper,
)

DESCRIPTOR: _descriptor.FileDescriptor

class StreamRequestType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    StreamRequestTypeUnknown: _ClassVar[StreamRequestType]
    ReceiveMessage: _ClassVar[StreamRequestType]
    AckMessage: _ClassVar[StreamRequestType]
    RejectMessage: _ClassVar[StreamRequestType]
    ModifyVisibility: _ClassVar[StreamRequestType]
    ResendMessage: _ClassVar[StreamRequestType]
    SendModifiedMessage: _ClassVar[StreamRequestType]

class QueuesDownstreamRequestType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PollRequestTypeUnknown: _ClassVar[QueuesDownstreamRequestType]
    Get: _ClassVar[QueuesDownstreamRequestType]
    AckAll: _ClassVar[QueuesDownstreamRequestType]
    AckRange: _ClassVar[QueuesDownstreamRequestType]
    NAckAll: _ClassVar[QueuesDownstreamRequestType]
    NAckRange: _ClassVar[QueuesDownstreamRequestType]
    ReQueueAll: _ClassVar[QueuesDownstreamRequestType]
    ReQueueRange: _ClassVar[QueuesDownstreamRequestType]
    ActiveOffsets: _ClassVar[QueuesDownstreamRequestType]
    TransactionStatus: _ClassVar[QueuesDownstreamRequestType]
    CloseByClient: _ClassVar[QueuesDownstreamRequestType]
    CloseByServer: _ClassVar[QueuesDownstreamRequestType]

StreamRequestTypeUnknown: StreamRequestType
ReceiveMessage: StreamRequestType
AckMessage: StreamRequestType
RejectMessage: StreamRequestType
ModifyVisibility: StreamRequestType
ResendMessage: StreamRequestType
SendModifiedMessage: StreamRequestType
PollRequestTypeUnknown: QueuesDownstreamRequestType
Get: QueuesDownstreamRequestType
AckAll: QueuesDownstreamRequestType
AckRange: QueuesDownstreamRequestType
NAckAll: QueuesDownstreamRequestType
NAckRange: QueuesDownstreamRequestType
ReQueueAll: QueuesDownstreamRequestType
ReQueueRange: QueuesDownstreamRequestType
ActiveOffsets: QueuesDownstreamRequestType
TransactionStatus: QueuesDownstreamRequestType
CloseByClient: QueuesDownstreamRequestType
CloseByServer: QueuesDownstreamRequestType

class PingResult(_message.Message):
    __slots__ = ("Host", "Version", "ServerStartTime", "ServerUpTimeSeconds")
    HOST_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SERVERSTARTTIME_FIELD_NUMBER: _ClassVar[int]
    SERVERUPTIMESECONDS_FIELD_NUMBER: _ClassVar[int]
    Host: str
    Version: str
    ServerStartTime: int
    ServerUpTimeSeconds: int
    def __init__(
        self,
        Host: str | None = ...,
        Version: str | None = ...,
        ServerStartTime: int | None = ...,
        ServerUpTimeSeconds: int | None = ...,
    ) -> None: ...

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Result(_message.Message):
    __slots__ = ("EventID", "Sent", "Error")
    EVENTID_FIELD_NUMBER: _ClassVar[int]
    SENT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    EventID: str
    Sent: bool
    Error: str
    def __init__(
        self,
        EventID: str | None = ...,
        Sent: bool = ...,
        Error: str | None = ...,
    ) -> None: ...

class Event(_message.Message):
    __slots__ = ("EventID", "ClientID", "Channel", "Metadata", "Body", "Store", "Tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...

    EVENTID_FIELD_NUMBER: _ClassVar[int]
    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    STORE_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    EventID: str
    ClientID: str
    Channel: str
    Metadata: str
    Body: bytes
    Store: bool
    Tags: _containers.ScalarMap[str, str]
    def __init__(
        self,
        EventID: str | None = ...,
        ClientID: str | None = ...,
        Channel: str | None = ...,
        Metadata: str | None = ...,
        Body: bytes | None = ...,
        Store: bool = ...,
        Tags: _Mapping[str, str] | None = ...,
    ) -> None: ...

class EventReceive(_message.Message):
    __slots__ = (
        "EventID",
        "Channel",
        "Metadata",
        "Body",
        "Timestamp",
        "Sequence",
        "Tags",
    )
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...

    EVENTID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    EventID: str
    Channel: str
    Metadata: str
    Body: bytes
    Timestamp: int
    Sequence: int
    Tags: _containers.ScalarMap[str, str]
    def __init__(
        self,
        EventID: str | None = ...,
        Channel: str | None = ...,
        Metadata: str | None = ...,
        Body: bytes | None = ...,
        Timestamp: int | None = ...,
        Sequence: int | None = ...,
        Tags: _Mapping[str, str] | None = ...,
    ) -> None: ...

class Subscribe(_message.Message):
    __slots__ = (
        "SubscribeTypeData",
        "ClientID",
        "Channel",
        "Group",
        "EventsStoreTypeData",
        "EventsStoreTypeValue",
    )
    class SubscribeType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SubscribeTypeUndefined: _ClassVar[Subscribe.SubscribeType]
        Events: _ClassVar[Subscribe.SubscribeType]
        EventsStore: _ClassVar[Subscribe.SubscribeType]
        Commands: _ClassVar[Subscribe.SubscribeType]
        Queries: _ClassVar[Subscribe.SubscribeType]

    SubscribeTypeUndefined: Subscribe.SubscribeType
    Events: Subscribe.SubscribeType
    EventsStore: Subscribe.SubscribeType
    Commands: Subscribe.SubscribeType
    Queries: Subscribe.SubscribeType
    class EventsStoreType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        EventsStoreTypeUndefined: _ClassVar[Subscribe.EventsStoreType]
        StartNewOnly: _ClassVar[Subscribe.EventsStoreType]
        StartFromFirst: _ClassVar[Subscribe.EventsStoreType]
        StartFromLast: _ClassVar[Subscribe.EventsStoreType]
        StartAtSequence: _ClassVar[Subscribe.EventsStoreType]
        StartAtTime: _ClassVar[Subscribe.EventsStoreType]
        StartAtTimeDelta: _ClassVar[Subscribe.EventsStoreType]

    EventsStoreTypeUndefined: Subscribe.EventsStoreType
    StartNewOnly: Subscribe.EventsStoreType
    StartFromFirst: Subscribe.EventsStoreType
    StartFromLast: Subscribe.EventsStoreType
    StartAtSequence: Subscribe.EventsStoreType
    StartAtTime: Subscribe.EventsStoreType
    StartAtTimeDelta: Subscribe.EventsStoreType
    SUBSCRIBETYPEDATA_FIELD_NUMBER: _ClassVar[int]
    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    EVENTSSTORETYPEDATA_FIELD_NUMBER: _ClassVar[int]
    EVENTSSTORETYPEVALUE_FIELD_NUMBER: _ClassVar[int]
    SubscribeTypeData: Subscribe.SubscribeType
    ClientID: str
    Channel: str
    Group: str
    EventsStoreTypeData: Subscribe.EventsStoreType
    EventsStoreTypeValue: int
    def __init__(
        self,
        SubscribeTypeData: Subscribe.SubscribeType | str | None = ...,
        ClientID: str | None = ...,
        Channel: str | None = ...,
        Group: str | None = ...,
        EventsStoreTypeData: Subscribe.EventsStoreType | str | None = ...,
        EventsStoreTypeValue: int | None = ...,
    ) -> None: ...

class Request(_message.Message):
    __slots__ = (
        "RequestID",
        "RequestTypeData",
        "ClientID",
        "Channel",
        "Metadata",
        "Body",
        "ReplyChannel",
        "Timeout",
        "CacheKey",
        "CacheTTL",
        "Span",
        "Tags",
    )
    class RequestType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        RequestTypeUnknown: _ClassVar[Request.RequestType]
        Command: _ClassVar[Request.RequestType]
        Query: _ClassVar[Request.RequestType]

    RequestTypeUnknown: Request.RequestType
    Command: Request.RequestType
    Query: Request.RequestType
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...

    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    REQUESTTYPEDATA_FIELD_NUMBER: _ClassVar[int]
    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    REPLYCHANNEL_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    CACHEKEY_FIELD_NUMBER: _ClassVar[int]
    CACHETTL_FIELD_NUMBER: _ClassVar[int]
    SPAN_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    RequestTypeData: Request.RequestType
    ClientID: str
    Channel: str
    Metadata: str
    Body: bytes
    ReplyChannel: str
    Timeout: int
    CacheKey: str
    CacheTTL: int
    Span: bytes
    Tags: _containers.ScalarMap[str, str]
    def __init__(
        self,
        RequestID: str | None = ...,
        RequestTypeData: Request.RequestType | str | None = ...,
        ClientID: str | None = ...,
        Channel: str | None = ...,
        Metadata: str | None = ...,
        Body: bytes | None = ...,
        ReplyChannel: str | None = ...,
        Timeout: int | None = ...,
        CacheKey: str | None = ...,
        CacheTTL: int | None = ...,
        Span: bytes | None = ...,
        Tags: _Mapping[str, str] | None = ...,
    ) -> None: ...

class Response(_message.Message):
    __slots__ = (
        "ClientID",
        "RequestID",
        "ReplyChannel",
        "Metadata",
        "Body",
        "CacheHit",
        "Timestamp",
        "Executed",
        "Error",
        "Span",
        "Tags",
    )
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...

    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    REPLYCHANNEL_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    CACHEHIT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    EXECUTED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    SPAN_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    ClientID: str
    RequestID: str
    ReplyChannel: str
    Metadata: str
    Body: bytes
    CacheHit: bool
    Timestamp: int
    Executed: bool
    Error: str
    Span: bytes
    Tags: _containers.ScalarMap[str, str]
    def __init__(
        self,
        ClientID: str | None = ...,
        RequestID: str | None = ...,
        ReplyChannel: str | None = ...,
        Metadata: str | None = ...,
        Body: bytes | None = ...,
        CacheHit: bool = ...,
        Timestamp: int | None = ...,
        Executed: bool = ...,
        Error: str | None = ...,
        Span: bytes | None = ...,
        Tags: _Mapping[str, str] | None = ...,
    ) -> None: ...

class QueueMessage(_message.Message):
    __slots__ = (
        "MessageID",
        "ClientID",
        "Channel",
        "Metadata",
        "Body",
        "Tags",
        "Attributes",
        "Policy",
        "Topic",
        "Partition",
        "PartitionKey",
    )
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...

    MESSAGEID_FIELD_NUMBER: _ClassVar[int]
    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    PARTITIONKEY_FIELD_NUMBER: _ClassVar[int]
    MessageID: str
    ClientID: str
    Channel: str
    Metadata: str
    Body: bytes
    Tags: _containers.ScalarMap[str, str]
    Attributes: QueueMessageAttributes
    Policy: QueueMessagePolicy
    Topic: str
    Partition: int
    PartitionKey: str
    def __init__(
        self,
        MessageID: str | None = ...,
        ClientID: str | None = ...,
        Channel: str | None = ...,
        Metadata: str | None = ...,
        Body: bytes | None = ...,
        Tags: _Mapping[str, str] | None = ...,
        Attributes: QueueMessageAttributes | _Mapping | None = ...,
        Policy: QueueMessagePolicy | _Mapping | None = ...,
        Topic: str | None = ...,
        Partition: int | None = ...,
        PartitionKey: str | None = ...,
    ) -> None: ...

class QueueMessagesBatchRequest(_message.Message):
    __slots__ = ("BatchID", "Messages")
    BATCHID_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    BatchID: str
    Messages: _containers.RepeatedCompositeFieldContainer[QueueMessage]
    def __init__(
        self,
        BatchID: str | None = ...,
        Messages: _Iterable[QueueMessage | _Mapping] | None = ...,
    ) -> None: ...

class QueueMessagesBatchResponse(_message.Message):
    __slots__ = ("BatchID", "Results", "HaveErrors")
    BATCHID_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    HAVEERRORS_FIELD_NUMBER: _ClassVar[int]
    BatchID: str
    Results: _containers.RepeatedCompositeFieldContainer[SendQueueMessageResult]
    HaveErrors: bool
    def __init__(
        self,
        BatchID: str | None = ...,
        Results: _Iterable[SendQueueMessageResult | _Mapping] | None = ...,
        HaveErrors: bool = ...,
    ) -> None: ...

class QueueMessageAttributes(_message.Message):
    __slots__ = (
        "Timestamp",
        "Sequence",
        "MD5OfBody",
        "ReceiveCount",
        "ReRouted",
        "ReRoutedFromQueue",
        "ExpirationAt",
        "DelayedTo",
    )
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    MD5OFBODY_FIELD_NUMBER: _ClassVar[int]
    RECEIVECOUNT_FIELD_NUMBER: _ClassVar[int]
    REROUTED_FIELD_NUMBER: _ClassVar[int]
    REROUTEDFROMQUEUE_FIELD_NUMBER: _ClassVar[int]
    EXPIRATIONAT_FIELD_NUMBER: _ClassVar[int]
    DELAYEDTO_FIELD_NUMBER: _ClassVar[int]
    Timestamp: int
    Sequence: int
    MD5OfBody: str
    ReceiveCount: int
    ReRouted: bool
    ReRoutedFromQueue: str
    ExpirationAt: int
    DelayedTo: int
    def __init__(
        self,
        Timestamp: int | None = ...,
        Sequence: int | None = ...,
        MD5OfBody: str | None = ...,
        ReceiveCount: int | None = ...,
        ReRouted: bool = ...,
        ReRoutedFromQueue: str | None = ...,
        ExpirationAt: int | None = ...,
        DelayedTo: int | None = ...,
    ) -> None: ...

class QueueMessagePolicy(_message.Message):
    __slots__ = (
        "ExpirationSeconds",
        "DelaySeconds",
        "MaxReceiveCount",
        "MaxReceiveQueue",
    )
    EXPIRATIONSECONDS_FIELD_NUMBER: _ClassVar[int]
    DELAYSECONDS_FIELD_NUMBER: _ClassVar[int]
    MAXRECEIVECOUNT_FIELD_NUMBER: _ClassVar[int]
    MAXRECEIVEQUEUE_FIELD_NUMBER: _ClassVar[int]
    ExpirationSeconds: int
    DelaySeconds: int
    MaxReceiveCount: int
    MaxReceiveQueue: str
    def __init__(
        self,
        ExpirationSeconds: int | None = ...,
        DelaySeconds: int | None = ...,
        MaxReceiveCount: int | None = ...,
        MaxReceiveQueue: str | None = ...,
    ) -> None: ...

class SendQueueMessageResult(_message.Message):
    __slots__ = (
        "MessageID",
        "SentAt",
        "ExpirationAt",
        "DelayedTo",
        "IsError",
        "Error",
        "RefChannel",
        "RefTopic",
        "RefPartition",
        "RefHash",
    )
    MESSAGEID_FIELD_NUMBER: _ClassVar[int]
    SENTAT_FIELD_NUMBER: _ClassVar[int]
    EXPIRATIONAT_FIELD_NUMBER: _ClassVar[int]
    DELAYEDTO_FIELD_NUMBER: _ClassVar[int]
    ISERROR_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    REFCHANNEL_FIELD_NUMBER: _ClassVar[int]
    REFTOPIC_FIELD_NUMBER: _ClassVar[int]
    REFPARTITION_FIELD_NUMBER: _ClassVar[int]
    REFHASH_FIELD_NUMBER: _ClassVar[int]
    MessageID: str
    SentAt: int
    ExpirationAt: int
    DelayedTo: int
    IsError: bool
    Error: str
    RefChannel: str
    RefTopic: str
    RefPartition: int
    RefHash: str
    def __init__(
        self,
        MessageID: str | None = ...,
        SentAt: int | None = ...,
        ExpirationAt: int | None = ...,
        DelayedTo: int | None = ...,
        IsError: bool = ...,
        Error: str | None = ...,
        RefChannel: str | None = ...,
        RefTopic: str | None = ...,
        RefPartition: int | None = ...,
        RefHash: str | None = ...,
    ) -> None: ...

class ReceiveQueueMessagesRequest(_message.Message):
    __slots__ = (
        "RequestID",
        "ClientID",
        "Channel",
        "MaxNumberOfMessages",
        "WaitTimeSeconds",
        "IsPeak",
    )
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    MAXNUMBEROFMESSAGES_FIELD_NUMBER: _ClassVar[int]
    WAITTIMESECONDS_FIELD_NUMBER: _ClassVar[int]
    ISPEAK_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    ClientID: str
    Channel: str
    MaxNumberOfMessages: int
    WaitTimeSeconds: int
    IsPeak: bool
    def __init__(
        self,
        RequestID: str | None = ...,
        ClientID: str | None = ...,
        Channel: str | None = ...,
        MaxNumberOfMessages: int | None = ...,
        WaitTimeSeconds: int | None = ...,
        IsPeak: bool = ...,
    ) -> None: ...

class ReceiveQueueMessagesResponse(_message.Message):
    __slots__ = (
        "RequestID",
        "Messages",
        "MessagesReceived",
        "MessagesExpired",
        "IsPeak",
        "IsError",
        "Error",
    )
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    MESSAGESRECEIVED_FIELD_NUMBER: _ClassVar[int]
    MESSAGESEXPIRED_FIELD_NUMBER: _ClassVar[int]
    ISPEAK_FIELD_NUMBER: _ClassVar[int]
    ISERROR_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    Messages: _containers.RepeatedCompositeFieldContainer[QueueMessage]
    MessagesReceived: int
    MessagesExpired: int
    IsPeak: bool
    IsError: bool
    Error: str
    def __init__(
        self,
        RequestID: str | None = ...,
        Messages: _Iterable[QueueMessage | _Mapping] | None = ...,
        MessagesReceived: int | None = ...,
        MessagesExpired: int | None = ...,
        IsPeak: bool = ...,
        IsError: bool = ...,
        Error: str | None = ...,
    ) -> None: ...

class AckAllQueueMessagesRequest(_message.Message):
    __slots__ = ("RequestID", "ClientID", "Channel", "WaitTimeSeconds")
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    WAITTIMESECONDS_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    ClientID: str
    Channel: str
    WaitTimeSeconds: int
    def __init__(
        self,
        RequestID: str | None = ...,
        ClientID: str | None = ...,
        Channel: str | None = ...,
        WaitTimeSeconds: int | None = ...,
    ) -> None: ...

class AckAllQueueMessagesResponse(_message.Message):
    __slots__ = ("RequestID", "AffectedMessages", "IsError", "Error")
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    AFFECTEDMESSAGES_FIELD_NUMBER: _ClassVar[int]
    ISERROR_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    AffectedMessages: int
    IsError: bool
    Error: str
    def __init__(
        self,
        RequestID: str | None = ...,
        AffectedMessages: int | None = ...,
        IsError: bool = ...,
        Error: str | None = ...,
    ) -> None: ...

class StreamQueueMessagesRequest(_message.Message):
    __slots__ = (
        "RequestID",
        "ClientID",
        "StreamRequestTypeData",
        "Channel",
        "VisibilitySeconds",
        "WaitTimeSeconds",
        "RefSequence",
        "ModifiedMessage",
    )
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    STREAMREQUESTTYPEDATA_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    VISIBILITYSECONDS_FIELD_NUMBER: _ClassVar[int]
    WAITTIMESECONDS_FIELD_NUMBER: _ClassVar[int]
    REFSEQUENCE_FIELD_NUMBER: _ClassVar[int]
    MODIFIEDMESSAGE_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    ClientID: str
    StreamRequestTypeData: StreamRequestType
    Channel: str
    VisibilitySeconds: int
    WaitTimeSeconds: int
    RefSequence: int
    ModifiedMessage: QueueMessage
    def __init__(
        self,
        RequestID: str | None = ...,
        ClientID: str | None = ...,
        StreamRequestTypeData: StreamRequestType | str | None = ...,
        Channel: str | None = ...,
        VisibilitySeconds: int | None = ...,
        WaitTimeSeconds: int | None = ...,
        RefSequence: int | None = ...,
        ModifiedMessage: QueueMessage | _Mapping | None = ...,
    ) -> None: ...

class StreamQueueMessagesResponse(_message.Message):
    __slots__ = ("RequestID", "StreamRequestTypeData", "Message", "IsError", "Error")
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    STREAMREQUESTTYPEDATA_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ISERROR_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    StreamRequestTypeData: StreamRequestType
    Message: QueueMessage
    IsError: bool
    Error: str
    def __init__(
        self,
        RequestID: str | None = ...,
        StreamRequestTypeData: StreamRequestType | str | None = ...,
        Message: QueueMessage | _Mapping | None = ...,
        IsError: bool = ...,
        Error: str | None = ...,
    ) -> None: ...

class QueuesUpstreamRequest(_message.Message):
    __slots__ = ("RequestID", "Messages")
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    Messages: _containers.RepeatedCompositeFieldContainer[QueueMessage]
    def __init__(
        self,
        RequestID: str | None = ...,
        Messages: _Iterable[QueueMessage | _Mapping] | None = ...,
    ) -> None: ...

class QueuesUpstreamResponse(_message.Message):
    __slots__ = ("RefRequestID", "Results", "IsError", "Error")
    REFREQUESTID_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ISERROR_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RefRequestID: str
    Results: _containers.RepeatedCompositeFieldContainer[SendQueueMessageResult]
    IsError: bool
    Error: str
    def __init__(
        self,
        RefRequestID: str | None = ...,
        Results: _Iterable[SendQueueMessageResult | _Mapping] | None = ...,
        IsError: bool = ...,
        Error: str | None = ...,
    ) -> None: ...

class QueuesDownstreamRequest(_message.Message):
    __slots__ = (
        "RequestID",
        "ClientID",
        "RequestTypeData",
        "Channel",
        "MaxItems",
        "WaitTimeout",
        "AutoAck",
        "ReQueueChannel",
        "SequenceRange",
        "RefTransactionId",
        "Metadata",
    )
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...

    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    REQUESTTYPEDATA_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    MAXITEMS_FIELD_NUMBER: _ClassVar[int]
    WAITTIMEOUT_FIELD_NUMBER: _ClassVar[int]
    AUTOACK_FIELD_NUMBER: _ClassVar[int]
    REQUEUECHANNEL_FIELD_NUMBER: _ClassVar[int]
    SEQUENCERANGE_FIELD_NUMBER: _ClassVar[int]
    REFTRANSACTIONID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    ClientID: str
    RequestTypeData: QueuesDownstreamRequestType
    Channel: str
    MaxItems: int
    WaitTimeout: int
    AutoAck: bool
    ReQueueChannel: str
    SequenceRange: _containers.RepeatedScalarFieldContainer[int]
    RefTransactionId: str
    Metadata: _containers.ScalarMap[str, str]
    def __init__(
        self,
        RequestID: str | None = ...,
        ClientID: str | None = ...,
        RequestTypeData: QueuesDownstreamRequestType | str | None = ...,
        Channel: str | None = ...,
        MaxItems: int | None = ...,
        WaitTimeout: int | None = ...,
        AutoAck: bool = ...,
        ReQueueChannel: str | None = ...,
        SequenceRange: _Iterable[int] | None = ...,
        RefTransactionId: str | None = ...,
        Metadata: _Mapping[str, str] | None = ...,
    ) -> None: ...

class QueuesDownstreamResponse(_message.Message):
    __slots__ = (
        "TransactionId",
        "RefRequestId",
        "RequestTypeData",
        "Messages",
        "ActiveOffsets",
        "IsError",
        "Error",
        "TransactionComplete",
        "Metadata",
    )
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...

    TRANSACTIONID_FIELD_NUMBER: _ClassVar[int]
    REFREQUESTID_FIELD_NUMBER: _ClassVar[int]
    REQUESTTYPEDATA_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    ACTIVEOFFSETS_FIELD_NUMBER: _ClassVar[int]
    ISERROR_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    TRANSACTIONCOMPLETE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    TransactionId: str
    RefRequestId: str
    RequestTypeData: QueuesDownstreamRequestType
    Messages: _containers.RepeatedCompositeFieldContainer[QueueMessage]
    ActiveOffsets: _containers.RepeatedScalarFieldContainer[int]
    IsError: bool
    Error: str
    TransactionComplete: bool
    Metadata: _containers.ScalarMap[str, str]
    def __init__(
        self,
        TransactionId: str | None = ...,
        RefRequestId: str | None = ...,
        RequestTypeData: QueuesDownstreamRequestType | str | None = ...,
        Messages: _Iterable[QueueMessage | _Mapping] | None = ...,
        ActiveOffsets: _Iterable[int] | None = ...,
        IsError: bool = ...,
        Error: str | None = ...,
        TransactionComplete: bool = ...,
        Metadata: _Mapping[str, str] | None = ...,
    ) -> None: ...

class QueueInfo(_message.Message):
    __slots__ = (
        "Name",
        "Messages",
        "Bytes",
        "FirstSequence",
        "LastSequence",
        "Sent",
        "Delivered",
        "Waiting",
        "Subscribers",
    )
    NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    FIRSTSEQUENCE_FIELD_NUMBER: _ClassVar[int]
    LASTSEQUENCE_FIELD_NUMBER: _ClassVar[int]
    SENT_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_FIELD_NUMBER: _ClassVar[int]
    WAITING_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIBERS_FIELD_NUMBER: _ClassVar[int]
    Name: str
    Messages: int
    Bytes: int
    FirstSequence: int
    LastSequence: int
    Sent: int
    Delivered: int
    Waiting: int
    Subscribers: int
    def __init__(
        self,
        Name: str | None = ...,
        Messages: int | None = ...,
        Bytes: int | None = ...,
        FirstSequence: int | None = ...,
        LastSequence: int | None = ...,
        Sent: int | None = ...,
        Delivered: int | None = ...,
        Waiting: int | None = ...,
        Subscribers: int | None = ...,
    ) -> None: ...

class QueuesInfo(_message.Message):
    __slots__ = ("TotalQueue", "Sent", "Delivered", "Waiting", "Queues")
    TOTALQUEUE_FIELD_NUMBER: _ClassVar[int]
    SENT_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_FIELD_NUMBER: _ClassVar[int]
    WAITING_FIELD_NUMBER: _ClassVar[int]
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    TotalQueue: int
    Sent: int
    Delivered: int
    Waiting: int
    Queues: _containers.RepeatedCompositeFieldContainer[QueueInfo]
    def __init__(
        self,
        TotalQueue: int | None = ...,
        Sent: int | None = ...,
        Delivered: int | None = ...,
        Waiting: int | None = ...,
        Queues: _Iterable[QueueInfo | _Mapping] | None = ...,
    ) -> None: ...

class QueuesInfoRequest(_message.Message):
    __slots__ = ("RequestID", "QueueName")
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    QUEUENAME_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    QueueName: str
    def __init__(self, RequestID: str | None = ..., QueueName: str | None = ...) -> None: ...

class QueuesInfoResponse(_message.Message):
    __slots__ = ("RefRequestID", "Info")
    REFREQUESTID_FIELD_NUMBER: _ClassVar[int]
    INFO_FIELD_NUMBER: _ClassVar[int]
    RefRequestID: str
    Info: QueuesInfo
    def __init__(
        self,
        RefRequestID: str | None = ...,
        Info: QueuesInfo | _Mapping | None = ...,
    ) -> None: ...
