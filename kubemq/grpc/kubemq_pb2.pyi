from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

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
    def __init__(self, Host: _Optional[str] = ..., Version: _Optional[str] = ..., ServerStartTime: _Optional[int] = ..., ServerUpTimeSeconds: _Optional[int] = ...) -> None: ...

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
    def __init__(self, EventID: _Optional[str] = ..., Sent: bool = ..., Error: _Optional[str] = ...) -> None: ...

class Event(_message.Message):
    __slots__ = ("EventID", "ClientID", "Channel", "Metadata", "Body", "Store", "Tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
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
    def __init__(self, EventID: _Optional[str] = ..., ClientID: _Optional[str] = ..., Channel: _Optional[str] = ..., Metadata: _Optional[str] = ..., Body: _Optional[bytes] = ..., Store: bool = ..., Tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class EventReceive(_message.Message):
    __slots__ = ("EventID", "Channel", "Metadata", "Body", "Timestamp", "Sequence", "Tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
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
    def __init__(self, EventID: _Optional[str] = ..., Channel: _Optional[str] = ..., Metadata: _Optional[str] = ..., Body: _Optional[bytes] = ..., Timestamp: _Optional[int] = ..., Sequence: _Optional[int] = ..., Tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class Subscribe(_message.Message):
    __slots__ = ("SubscribeTypeData", "ClientID", "Channel", "Group", "EventsStoreTypeData", "EventsStoreTypeValue")
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
    def __init__(self, SubscribeTypeData: _Optional[_Union[Subscribe.SubscribeType, str]] = ..., ClientID: _Optional[str] = ..., Channel: _Optional[str] = ..., Group: _Optional[str] = ..., EventsStoreTypeData: _Optional[_Union[Subscribe.EventsStoreType, str]] = ..., EventsStoreTypeValue: _Optional[int] = ...) -> None: ...

class Request(_message.Message):
    __slots__ = ("RequestID", "RequestTypeData", "ClientID", "Channel", "Metadata", "Body", "ReplyChannel", "Timeout", "CacheKey", "CacheTTL", "Span", "Tags")
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
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
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
    def __init__(self, RequestID: _Optional[str] = ..., RequestTypeData: _Optional[_Union[Request.RequestType, str]] = ..., ClientID: _Optional[str] = ..., Channel: _Optional[str] = ..., Metadata: _Optional[str] = ..., Body: _Optional[bytes] = ..., ReplyChannel: _Optional[str] = ..., Timeout: _Optional[int] = ..., CacheKey: _Optional[str] = ..., CacheTTL: _Optional[int] = ..., Span: _Optional[bytes] = ..., Tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class Response(_message.Message):
    __slots__ = ("ClientID", "RequestID", "ReplyChannel", "Metadata", "Body", "CacheHit", "Timestamp", "Executed", "Error", "Span", "Tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
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
    def __init__(self, ClientID: _Optional[str] = ..., RequestID: _Optional[str] = ..., ReplyChannel: _Optional[str] = ..., Metadata: _Optional[str] = ..., Body: _Optional[bytes] = ..., CacheHit: bool = ..., Timestamp: _Optional[int] = ..., Executed: bool = ..., Error: _Optional[str] = ..., Span: _Optional[bytes] = ..., Tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class QueueMessage(_message.Message):
    __slots__ = ("MessageID", "ClientID", "Channel", "Metadata", "Body", "Tags", "Attributes", "Policy", "Topic", "Partition", "PartitionKey")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
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
    def __init__(self, MessageID: _Optional[str] = ..., ClientID: _Optional[str] = ..., Channel: _Optional[str] = ..., Metadata: _Optional[str] = ..., Body: _Optional[bytes] = ..., Tags: _Optional[_Mapping[str, str]] = ..., Attributes: _Optional[_Union[QueueMessageAttributes, _Mapping]] = ..., Policy: _Optional[_Union[QueueMessagePolicy, _Mapping]] = ..., Topic: _Optional[str] = ..., Partition: _Optional[int] = ..., PartitionKey: _Optional[str] = ...) -> None: ...

class QueueMessagesBatchRequest(_message.Message):
    __slots__ = ("BatchID", "Messages")
    BATCHID_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    BatchID: str
    Messages: _containers.RepeatedCompositeFieldContainer[QueueMessage]
    def __init__(self, BatchID: _Optional[str] = ..., Messages: _Optional[_Iterable[_Union[QueueMessage, _Mapping]]] = ...) -> None: ...

class QueueMessagesBatchResponse(_message.Message):
    __slots__ = ("BatchID", "Results", "HaveErrors")
    BATCHID_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    HAVEERRORS_FIELD_NUMBER: _ClassVar[int]
    BatchID: str
    Results: _containers.RepeatedCompositeFieldContainer[SendQueueMessageResult]
    HaveErrors: bool
    def __init__(self, BatchID: _Optional[str] = ..., Results: _Optional[_Iterable[_Union[SendQueueMessageResult, _Mapping]]] = ..., HaveErrors: bool = ...) -> None: ...

class QueueMessageAttributes(_message.Message):
    __slots__ = ("Timestamp", "Sequence", "MD5OfBody", "ReceiveCount", "ReRouted", "ReRoutedFromQueue", "ExpirationAt", "DelayedTo")
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
    def __init__(self, Timestamp: _Optional[int] = ..., Sequence: _Optional[int] = ..., MD5OfBody: _Optional[str] = ..., ReceiveCount: _Optional[int] = ..., ReRouted: bool = ..., ReRoutedFromQueue: _Optional[str] = ..., ExpirationAt: _Optional[int] = ..., DelayedTo: _Optional[int] = ...) -> None: ...

class QueueMessagePolicy(_message.Message):
    __slots__ = ("ExpirationSeconds", "DelaySeconds", "MaxReceiveCount", "MaxReceiveQueue")
    EXPIRATIONSECONDS_FIELD_NUMBER: _ClassVar[int]
    DELAYSECONDS_FIELD_NUMBER: _ClassVar[int]
    MAXRECEIVECOUNT_FIELD_NUMBER: _ClassVar[int]
    MAXRECEIVEQUEUE_FIELD_NUMBER: _ClassVar[int]
    ExpirationSeconds: int
    DelaySeconds: int
    MaxReceiveCount: int
    MaxReceiveQueue: str
    def __init__(self, ExpirationSeconds: _Optional[int] = ..., DelaySeconds: _Optional[int] = ..., MaxReceiveCount: _Optional[int] = ..., MaxReceiveQueue: _Optional[str] = ...) -> None: ...

class SendQueueMessageResult(_message.Message):
    __slots__ = ("MessageID", "SentAt", "ExpirationAt", "DelayedTo", "IsError", "Error", "RefChannel", "RefTopic", "RefPartition", "RefHash")
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
    def __init__(self, MessageID: _Optional[str] = ..., SentAt: _Optional[int] = ..., ExpirationAt: _Optional[int] = ..., DelayedTo: _Optional[int] = ..., IsError: bool = ..., Error: _Optional[str] = ..., RefChannel: _Optional[str] = ..., RefTopic: _Optional[str] = ..., RefPartition: _Optional[int] = ..., RefHash: _Optional[str] = ...) -> None: ...

class ReceiveQueueMessagesRequest(_message.Message):
    __slots__ = ("RequestID", "ClientID", "Channel", "MaxNumberOfMessages", "WaitTimeSeconds", "IsPeak")
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
    def __init__(self, RequestID: _Optional[str] = ..., ClientID: _Optional[str] = ..., Channel: _Optional[str] = ..., MaxNumberOfMessages: _Optional[int] = ..., WaitTimeSeconds: _Optional[int] = ..., IsPeak: bool = ...) -> None: ...

class ReceiveQueueMessagesResponse(_message.Message):
    __slots__ = ("RequestID", "Messages", "MessagesReceived", "MessagesExpired", "IsPeak", "IsError", "Error")
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
    def __init__(self, RequestID: _Optional[str] = ..., Messages: _Optional[_Iterable[_Union[QueueMessage, _Mapping]]] = ..., MessagesReceived: _Optional[int] = ..., MessagesExpired: _Optional[int] = ..., IsPeak: bool = ..., IsError: bool = ..., Error: _Optional[str] = ...) -> None: ...

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
    def __init__(self, RequestID: _Optional[str] = ..., ClientID: _Optional[str] = ..., Channel: _Optional[str] = ..., WaitTimeSeconds: _Optional[int] = ...) -> None: ...

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
    def __init__(self, RequestID: _Optional[str] = ..., AffectedMessages: _Optional[int] = ..., IsError: bool = ..., Error: _Optional[str] = ...) -> None: ...

class StreamQueueMessagesRequest(_message.Message):
    __slots__ = ("RequestID", "ClientID", "StreamRequestTypeData", "Channel", "VisibilitySeconds", "WaitTimeSeconds", "RefSequence", "ModifiedMessage")
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
    def __init__(self, RequestID: _Optional[str] = ..., ClientID: _Optional[str] = ..., StreamRequestTypeData: _Optional[_Union[StreamRequestType, str]] = ..., Channel: _Optional[str] = ..., VisibilitySeconds: _Optional[int] = ..., WaitTimeSeconds: _Optional[int] = ..., RefSequence: _Optional[int] = ..., ModifiedMessage: _Optional[_Union[QueueMessage, _Mapping]] = ...) -> None: ...

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
    def __init__(self, RequestID: _Optional[str] = ..., StreamRequestTypeData: _Optional[_Union[StreamRequestType, str]] = ..., Message: _Optional[_Union[QueueMessage, _Mapping]] = ..., IsError: bool = ..., Error: _Optional[str] = ...) -> None: ...

class QueuesUpstreamRequest(_message.Message):
    __slots__ = ("RequestID", "Messages")
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    Messages: _containers.RepeatedCompositeFieldContainer[QueueMessage]
    def __init__(self, RequestID: _Optional[str] = ..., Messages: _Optional[_Iterable[_Union[QueueMessage, _Mapping]]] = ...) -> None: ...

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
    def __init__(self, RefRequestID: _Optional[str] = ..., Results: _Optional[_Iterable[_Union[SendQueueMessageResult, _Mapping]]] = ..., IsError: bool = ..., Error: _Optional[str] = ...) -> None: ...

class QueuesDownstreamRequest(_message.Message):
    __slots__ = ("RequestID", "ClientID", "RequestTypeData", "Channel", "MaxItems", "WaitTimeout", "AutoAck", "ReQueueChannel", "SequenceRange", "RefTransactionId", "Metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
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
    def __init__(self, RequestID: _Optional[str] = ..., ClientID: _Optional[str] = ..., RequestTypeData: _Optional[_Union[QueuesDownstreamRequestType, str]] = ..., Channel: _Optional[str] = ..., MaxItems: _Optional[int] = ..., WaitTimeout: _Optional[int] = ..., AutoAck: bool = ..., ReQueueChannel: _Optional[str] = ..., SequenceRange: _Optional[_Iterable[int]] = ..., RefTransactionId: _Optional[str] = ..., Metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class QueuesDownstreamResponse(_message.Message):
    __slots__ = ("TransactionId", "RefRequestId", "RequestTypeData", "Messages", "ActiveOffsets", "IsError", "Error", "TransactionComplete", "Metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
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
    def __init__(self, TransactionId: _Optional[str] = ..., RefRequestId: _Optional[str] = ..., RequestTypeData: _Optional[_Union[QueuesDownstreamRequestType, str]] = ..., Messages: _Optional[_Iterable[_Union[QueueMessage, _Mapping]]] = ..., ActiveOffsets: _Optional[_Iterable[int]] = ..., IsError: bool = ..., Error: _Optional[str] = ..., TransactionComplete: bool = ..., Metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class QueueInfo(_message.Message):
    __slots__ = ("Name", "Messages", "Bytes", "FirstSequence", "LastSequence", "Sent", "Delivered", "Waiting", "Subscribers")
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
    def __init__(self, Name: _Optional[str] = ..., Messages: _Optional[int] = ..., Bytes: _Optional[int] = ..., FirstSequence: _Optional[int] = ..., LastSequence: _Optional[int] = ..., Sent: _Optional[int] = ..., Delivered: _Optional[int] = ..., Waiting: _Optional[int] = ..., Subscribers: _Optional[int] = ...) -> None: ...

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
    def __init__(self, TotalQueue: _Optional[int] = ..., Sent: _Optional[int] = ..., Delivered: _Optional[int] = ..., Waiting: _Optional[int] = ..., Queues: _Optional[_Iterable[_Union[QueueInfo, _Mapping]]] = ...) -> None: ...

class QueuesInfoRequest(_message.Message):
    __slots__ = ("RequestID", "QueueName")
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    QUEUENAME_FIELD_NUMBER: _ClassVar[int]
    RequestID: str
    QueueName: str
    def __init__(self, RequestID: _Optional[str] = ..., QueueName: _Optional[str] = ...) -> None: ...

class QueuesInfoResponse(_message.Message):
    __slots__ = ("RefRequestID", "Info")
    REFREQUESTID_FIELD_NUMBER: _ClassVar[int]
    INFO_FIELD_NUMBER: _ClassVar[int]
    RefRequestID: str
    Info: QueuesInfo
    def __init__(self, RefRequestID: _Optional[str] = ..., Info: _Optional[_Union[QueuesInfo, _Mapping]] = ...) -> None: ...
