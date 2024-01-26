# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: kubemq/grpc/kubemq.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x18kubemq/grpc/kubemq.proto\x12\x06kubemq\"a\n\nPingResult\x12\x0c\n\x04Host\x18\x01 \x01(\t\x12\x0f\n\x07Version\x18\x02 \x01(\t\x12\x17\n\x0fServerStartTime\x18\x03 \x01(\x03\x12\x1b\n\x13ServerUpTimeSeconds\x18\x04 \x01(\x03\"\x07\n\x05\x45mpty\"6\n\x06Result\x12\x0f\n\x07\x45ventID\x18\x01 \x01(\t\x12\x0c\n\x04Sent\x18\x02 \x01(\x08\x12\r\n\x05\x45rror\x18\x03 \x01(\t\"\xbe\x01\n\x05\x45vent\x12\x0f\n\x07\x45ventID\x18\x01 \x01(\t\x12\x10\n\x08\x43lientID\x18\x02 \x01(\t\x12\x0f\n\x07\x43hannel\x18\x03 \x01(\t\x12\x10\n\x08Metadata\x18\x04 \x01(\t\x12\x0c\n\x04\x42ody\x18\x05 \x01(\x0c\x12\r\n\x05Store\x18\x06 \x01(\x08\x12%\n\x04Tags\x18\x07 \x03(\x0b\x32\x17.kubemq.Event.TagsEntry\x1a+\n\tTagsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xd0\x01\n\x0c\x45ventReceive\x12\x0f\n\x07\x45ventID\x18\x01 \x01(\t\x12\x0f\n\x07\x43hannel\x18\x02 \x01(\t\x12\x10\n\x08Metadata\x18\x03 \x01(\t\x12\x0c\n\x04\x42ody\x18\x04 \x01(\x0c\x12\x11\n\tTimestamp\x18\x05 \x01(\x03\x12\x10\n\x08Sequence\x18\x06 \x01(\x04\x12,\n\x04Tags\x18\x07 \x03(\x0b\x32\x1e.kubemq.EventReceive.TagsEntry\x1a+\n\tTagsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xe3\x03\n\tSubscribe\x12:\n\x11SubscribeTypeData\x18\x01 \x01(\x0e\x32\x1f.kubemq.Subscribe.SubscribeType\x12\x10\n\x08\x43lientID\x18\x02 \x01(\t\x12\x0f\n\x07\x43hannel\x18\x03 \x01(\t\x12\r\n\x05Group\x18\x04 \x01(\t\x12>\n\x13\x45ventsStoreTypeData\x18\x05 \x01(\x0e\x32!.kubemq.Subscribe.EventsStoreType\x12\x1c\n\x14\x45ventsStoreTypeValue\x18\x06 \x01(\x03\"c\n\rSubscribeType\x12\x1a\n\x16SubscribeTypeUndefined\x10\x00\x12\n\n\x06\x45vents\x10\x01\x12\x0f\n\x0b\x45ventsStore\x10\x02\x12\x0c\n\x08\x43ommands\x10\x03\x12\x0b\n\x07Queries\x10\x04\"\xa4\x01\n\x0f\x45ventsStoreType\x12\x1c\n\x18\x45ventsStoreTypeUndefined\x10\x00\x12\x10\n\x0cStartNewOnly\x10\x01\x12\x12\n\x0eStartFromFirst\x10\x02\x12\x11\n\rStartFromLast\x10\x03\x12\x13\n\x0fStartAtSequence\x10\x04\x12\x0f\n\x0bStartAtTime\x10\x05\x12\x14\n\x10StartAtTimeDelta\x10\x06\"\x83\x03\n\x07Request\x12\x11\n\tRequestID\x18\x01 \x01(\t\x12\x34\n\x0fRequestTypeData\x18\x02 \x01(\x0e\x32\x1b.kubemq.Request.RequestType\x12\x10\n\x08\x43lientID\x18\x03 \x01(\t\x12\x0f\n\x07\x43hannel\x18\x04 \x01(\t\x12\x10\n\x08Metadata\x18\x05 \x01(\t\x12\x0c\n\x04\x42ody\x18\x06 \x01(\x0c\x12\x14\n\x0cReplyChannel\x18\x07 \x01(\t\x12\x0f\n\x07Timeout\x18\x08 \x01(\x05\x12\x10\n\x08\x43\x61\x63heKey\x18\t \x01(\t\x12\x10\n\x08\x43\x61\x63heTTL\x18\n \x01(\x05\x12\x0c\n\x04Span\x18\x0b \x01(\x0c\x12\'\n\x04Tags\x18\x0c \x03(\x0b\x32\x19.kubemq.Request.TagsEntry\x1a+\n\tTagsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"=\n\x0bRequestType\x12\x16\n\x12RequestTypeUnknown\x10\x00\x12\x0b\n\x07\x43ommand\x10\x01\x12\t\n\x05Query\x10\x02\"\x90\x02\n\x08Response\x12\x10\n\x08\x43lientID\x18\x01 \x01(\t\x12\x11\n\tRequestID\x18\x02 \x01(\t\x12\x14\n\x0cReplyChannel\x18\x03 \x01(\t\x12\x10\n\x08Metadata\x18\x04 \x01(\t\x12\x0c\n\x04\x42ody\x18\x05 \x01(\x0c\x12\x10\n\x08\x43\x61\x63heHit\x18\x06 \x01(\x08\x12\x11\n\tTimestamp\x18\x07 \x01(\x03\x12\x10\n\x08\x45xecuted\x18\x08 \x01(\x08\x12\r\n\x05\x45rror\x18\t \x01(\t\x12\x0c\n\x04Span\x18\n \x01(\x0c\x12(\n\x04Tags\x18\x0b \x03(\x0b\x32\x1a.kubemq.Response.TagsEntry\x1a+\n\tTagsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x9f\x02\n\x0cQueueMessage\x12\x11\n\tMessageID\x18\x01 \x01(\t\x12\x10\n\x08\x43lientID\x18\x02 \x01(\t\x12\x0f\n\x07\x43hannel\x18\x03 \x01(\t\x12\x10\n\x08Metadata\x18\x04 \x01(\t\x12\x0c\n\x04\x42ody\x18\x05 \x01(\x0c\x12,\n\x04Tags\x18\x06 \x03(\x0b\x32\x1e.kubemq.QueueMessage.TagsEntry\x12\x32\n\nAttributes\x18\x07 \x01(\x0b\x32\x1e.kubemq.QueueMessageAttributes\x12*\n\x06Policy\x18\x08 \x01(\x0b\x32\x1a.kubemq.QueueMessagePolicy\x1a+\n\tTagsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"T\n\x19QueueMessagesBatchRequest\x12\x0f\n\x07\x42\x61tchID\x18\x01 \x01(\t\x12&\n\x08Messages\x18\x02 \x03(\x0b\x32\x14.kubemq.QueueMessage\"r\n\x1aQueueMessagesBatchResponse\x12\x0f\n\x07\x42\x61tchID\x18\x01 \x01(\t\x12/\n\x07Results\x18\x02 \x03(\x0b\x32\x1e.kubemq.SendQueueMessageResult\x12\x12\n\nHaveErrors\x18\x03 \x01(\x08\"\xbc\x01\n\x16QueueMessageAttributes\x12\x11\n\tTimestamp\x18\x01 \x01(\x03\x12\x10\n\x08Sequence\x18\x02 \x01(\x04\x12\x11\n\tMD5OfBody\x18\x03 \x01(\t\x12\x14\n\x0cReceiveCount\x18\x04 \x01(\x05\x12\x10\n\x08ReRouted\x18\x05 \x01(\x08\x12\x19\n\x11ReRoutedFromQueue\x18\x06 \x01(\t\x12\x14\n\x0c\x45xpirationAt\x18\x07 \x01(\x03\x12\x11\n\tDelayedTo\x18\x08 \x01(\x03\"w\n\x12QueueMessagePolicy\x12\x19\n\x11\x45xpirationSeconds\x18\x01 \x01(\x05\x12\x14\n\x0c\x44\x65laySeconds\x18\x02 \x01(\x05\x12\x17\n\x0fMaxReceiveCount\x18\x03 \x01(\x05\x12\x17\n\x0fMaxReceiveQueue\x18\x04 \x01(\t\"\x84\x01\n\x16SendQueueMessageResult\x12\x11\n\tMessageID\x18\x01 \x01(\t\x12\x0e\n\x06SentAt\x18\x02 \x01(\x03\x12\x14\n\x0c\x45xpirationAt\x18\x03 \x01(\x03\x12\x11\n\tDelayedTo\x18\x04 \x01(\x03\x12\x0f\n\x07IsError\x18\x05 \x01(\x08\x12\r\n\x05\x45rror\x18\x06 \x01(\t\"\x99\x01\n\x1bReceiveQueueMessagesRequest\x12\x11\n\tRequestID\x18\x01 \x01(\t\x12\x10\n\x08\x43lientID\x18\x02 \x01(\t\x12\x0f\n\x07\x43hannel\x18\x03 \x01(\t\x12\x1b\n\x13MaxNumberOfMessages\x18\x04 \x01(\x05\x12\x17\n\x0fWaitTimeSeconds\x18\x05 \x01(\x05\x12\x0e\n\x06IsPeak\x18\x06 \x01(\x08\"\xbc\x01\n\x1cReceiveQueueMessagesResponse\x12\x11\n\tRequestID\x18\x01 \x01(\t\x12&\n\x08Messages\x18\x02 \x03(\x0b\x32\x14.kubemq.QueueMessage\x12\x18\n\x10MessagesReceived\x18\x03 \x01(\x05\x12\x17\n\x0fMessagesExpired\x18\x04 \x01(\x05\x12\x0e\n\x06IsPeak\x18\x05 \x01(\x08\x12\x0f\n\x07IsError\x18\x06 \x01(\x08\x12\r\n\x05\x45rror\x18\x07 \x01(\t\"k\n\x1a\x41\x63kAllQueueMessagesRequest\x12\x11\n\tRequestID\x18\x01 \x01(\t\x12\x10\n\x08\x43lientID\x18\x02 \x01(\t\x12\x0f\n\x07\x43hannel\x18\x03 \x01(\t\x12\x17\n\x0fWaitTimeSeconds\x18\x04 \x01(\x05\"j\n\x1b\x41\x63kAllQueueMessagesResponse\x12\x11\n\tRequestID\x18\x01 \x01(\t\x12\x18\n\x10\x41\x66\x66\x65\x63tedMessages\x18\x02 \x01(\x04\x12\x0f\n\x07IsError\x18\x03 \x01(\x08\x12\r\n\x05\x45rror\x18\x04 \x01(\t\"\x84\x02\n\x1aStreamQueueMessagesRequest\x12\x11\n\tRequestID\x18\x01 \x01(\t\x12\x10\n\x08\x43lientID\x18\x02 \x01(\t\x12\x38\n\x15StreamRequestTypeData\x18\x03 \x01(\x0e\x32\x19.kubemq.StreamRequestType\x12\x0f\n\x07\x43hannel\x18\x04 \x01(\t\x12\x19\n\x11VisibilitySeconds\x18\x05 \x01(\x05\x12\x17\n\x0fWaitTimeSeconds\x18\x06 \x01(\x05\x12\x13\n\x0bRefSequence\x18\x07 \x01(\x04\x12-\n\x0fModifiedMessage\x18\x08 \x01(\x0b\x32\x14.kubemq.QueueMessage\"\xb1\x01\n\x1bStreamQueueMessagesResponse\x12\x11\n\tRequestID\x18\x01 \x01(\t\x12\x38\n\x15StreamRequestTypeData\x18\x02 \x01(\x0e\x32\x19.kubemq.StreamRequestType\x12%\n\x07Message\x18\x03 \x01(\x0b\x32\x14.kubemq.QueueMessage\x12\x0f\n\x07IsError\x18\x04 \x01(\x08\x12\r\n\x05\x45rror\x18\x05 \x01(\t*\xaa\x01\n\x11StreamRequestType\x12\x1c\n\x18StreamRequestTypeUnknown\x10\x00\x12\x12\n\x0eReceiveMessage\x10\x01\x12\x0e\n\nAckMessage\x10\x02\x12\x11\n\rRejectMessage\x10\x03\x12\x14\n\x10ModifyVisibility\x10\x04\x12\x11\n\rResendMessage\x10\x05\x12\x17\n\x13SendModifiedMessage\x10\x06\x32\xdf\x06\n\x06kubemq\x12,\n\tSendEvent\x12\r.kubemq.Event\x1a\x0e.kubemq.Result\"\x00\x12\x37\n\x10SendEventsStream\x12\r.kubemq.Event\x1a\x0e.kubemq.Result\"\x00(\x01\x30\x01\x12@\n\x11SubscribeToEvents\x12\x11.kubemq.Subscribe\x1a\x14.kubemq.EventReceive\"\x00\x30\x01\x12=\n\x13SubscribeToRequests\x12\x11.kubemq.Subscribe\x1a\x0f.kubemq.Request\"\x00\x30\x01\x12\x32\n\x0bSendRequest\x12\x0f.kubemq.Request\x1a\x10.kubemq.Response\"\x00\x12\x31\n\x0cSendResponse\x12\x10.kubemq.Response\x1a\r.kubemq.Empty\"\x00\x12J\n\x10SendQueueMessage\x12\x14.kubemq.QueueMessage\x1a\x1e.kubemq.SendQueueMessageResult\"\x00\x12\x61\n\x16SendQueueMessagesBatch\x12!.kubemq.QueueMessagesBatchRequest\x1a\".kubemq.QueueMessagesBatchResponse\"\x00\x12\x63\n\x14ReceiveQueueMessages\x12#.kubemq.ReceiveQueueMessagesRequest\x1a$.kubemq.ReceiveQueueMessagesResponse\"\x00\x12\x63\n\x12StreamQueueMessage\x12\".kubemq.StreamQueueMessagesRequest\x1a#.kubemq.StreamQueueMessagesResponse\"\x00(\x01\x30\x01\x12`\n\x13\x41\x63kAllQueueMessages\x12\".kubemq.AckAllQueueMessagesRequest\x1a#.kubemq.AckAllQueueMessagesResponse\"\x00\x12+\n\x04Ping\x12\r.kubemq.Empty\x1a\x12.kubemq.PingResult\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'kubemq.grpc.kubemq_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_EVENT_TAGSENTRY']._options = None
  _globals['_EVENT_TAGSENTRY']._serialized_options = b'8\001'
  _globals['_EVENTRECEIVE_TAGSENTRY']._options = None
  _globals['_EVENTRECEIVE_TAGSENTRY']._serialized_options = b'8\001'
  _globals['_REQUEST_TAGSENTRY']._options = None
  _globals['_REQUEST_TAGSENTRY']._serialized_options = b'8\001'
  _globals['_RESPONSE_TAGSENTRY']._options = None
  _globals['_RESPONSE_TAGSENTRY']._serialized_options = b'8\001'
  _globals['_QUEUEMESSAGE_TAGSENTRY']._options = None
  _globals['_QUEUEMESSAGE_TAGSENTRY']._serialized_options = b'8\001'
  _globals['_STREAMREQUESTTYPE']._serialized_start=3702
  _globals['_STREAMREQUESTTYPE']._serialized_end=3872
  _globals['_PINGRESULT']._serialized_start=36
  _globals['_PINGRESULT']._serialized_end=133
  _globals['_EMPTY']._serialized_start=135
  _globals['_EMPTY']._serialized_end=142
  _globals['_RESULT']._serialized_start=144
  _globals['_RESULT']._serialized_end=198
  _globals['_EVENT']._serialized_start=201
  _globals['_EVENT']._serialized_end=391
  _globals['_EVENT_TAGSENTRY']._serialized_start=348
  _globals['_EVENT_TAGSENTRY']._serialized_end=391
  _globals['_EVENTRECEIVE']._serialized_start=394
  _globals['_EVENTRECEIVE']._serialized_end=602
  _globals['_EVENTRECEIVE_TAGSENTRY']._serialized_start=348
  _globals['_EVENTRECEIVE_TAGSENTRY']._serialized_end=391
  _globals['_SUBSCRIBE']._serialized_start=605
  _globals['_SUBSCRIBE']._serialized_end=1088
  _globals['_SUBSCRIBE_SUBSCRIBETYPE']._serialized_start=822
  _globals['_SUBSCRIBE_SUBSCRIBETYPE']._serialized_end=921
  _globals['_SUBSCRIBE_EVENTSSTORETYPE']._serialized_start=924
  _globals['_SUBSCRIBE_EVENTSSTORETYPE']._serialized_end=1088
  _globals['_REQUEST']._serialized_start=1091
  _globals['_REQUEST']._serialized_end=1478
  _globals['_REQUEST_TAGSENTRY']._serialized_start=348
  _globals['_REQUEST_TAGSENTRY']._serialized_end=391
  _globals['_REQUEST_REQUESTTYPE']._serialized_start=1417
  _globals['_REQUEST_REQUESTTYPE']._serialized_end=1478
  _globals['_RESPONSE']._serialized_start=1481
  _globals['_RESPONSE']._serialized_end=1753
  _globals['_RESPONSE_TAGSENTRY']._serialized_start=348
  _globals['_RESPONSE_TAGSENTRY']._serialized_end=391
  _globals['_QUEUEMESSAGE']._serialized_start=1756
  _globals['_QUEUEMESSAGE']._serialized_end=2043
  _globals['_QUEUEMESSAGE_TAGSENTRY']._serialized_start=348
  _globals['_QUEUEMESSAGE_TAGSENTRY']._serialized_end=391
  _globals['_QUEUEMESSAGESBATCHREQUEST']._serialized_start=2045
  _globals['_QUEUEMESSAGESBATCHREQUEST']._serialized_end=2129
  _globals['_QUEUEMESSAGESBATCHRESPONSE']._serialized_start=2131
  _globals['_QUEUEMESSAGESBATCHRESPONSE']._serialized_end=2245
  _globals['_QUEUEMESSAGEATTRIBUTES']._serialized_start=2248
  _globals['_QUEUEMESSAGEATTRIBUTES']._serialized_end=2436
  _globals['_QUEUEMESSAGEPOLICY']._serialized_start=2438
  _globals['_QUEUEMESSAGEPOLICY']._serialized_end=2557
  _globals['_SENDQUEUEMESSAGERESULT']._serialized_start=2560
  _globals['_SENDQUEUEMESSAGERESULT']._serialized_end=2692
  _globals['_RECEIVEQUEUEMESSAGESREQUEST']._serialized_start=2695
  _globals['_RECEIVEQUEUEMESSAGESREQUEST']._serialized_end=2848
  _globals['_RECEIVEQUEUEMESSAGESRESPONSE']._serialized_start=2851
  _globals['_RECEIVEQUEUEMESSAGESRESPONSE']._serialized_end=3039
  _globals['_ACKALLQUEUEMESSAGESREQUEST']._serialized_start=3041
  _globals['_ACKALLQUEUEMESSAGESREQUEST']._serialized_end=3148
  _globals['_ACKALLQUEUEMESSAGESRESPONSE']._serialized_start=3150
  _globals['_ACKALLQUEUEMESSAGESRESPONSE']._serialized_end=3256
  _globals['_STREAMQUEUEMESSAGESREQUEST']._serialized_start=3259
  _globals['_STREAMQUEUEMESSAGESREQUEST']._serialized_end=3519
  _globals['_STREAMQUEUEMESSAGESRESPONSE']._serialized_start=3522
  _globals['_STREAMQUEUEMESSAGESRESPONSE']._serialized_end=3699
  _globals['_KUBEMQ']._serialized_start=3875
  _globals['_KUBEMQ']._serialized_end=4738
# @@protoc_insertion_point(module_scope)
