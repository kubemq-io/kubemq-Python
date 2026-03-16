"""OpenTelemetry semantic convention constants for KubeMQ.

Based on OTel messaging semconv v1.27.0.
All attribute names are defined here as constants to ensure consistency.
"""

from __future__ import annotations

# Messaging attributes
MESSAGING_SYSTEM = "messaging.system"
MESSAGING_SYSTEM_VALUE = "kubemq"
MESSAGING_OPERATION_NAME = "messaging.operation.name"
MESSAGING_OPERATION_TYPE = "messaging.operation.type"
MESSAGING_DESTINATION_NAME = "messaging.destination.name"
MESSAGING_MESSAGE_ID = "messaging.message.id"
MESSAGING_MESSAGE_BODY_SIZE = "messaging.message.body.size"
MESSAGING_CLIENT_ID = "messaging.client.id"
MESSAGING_CONSUMER_GROUP_NAME = "messaging.consumer.group.name"
MESSAGING_BATCH_MESSAGE_COUNT = "messaging.batch.message_count"

# Server attributes
SERVER_ADDRESS = "server.address"
SERVER_PORT = "server.port"

# Error attributes
ERROR_TYPE = "error.type"

# Retry event attributes
RETRY_EVENT_NAME = "retry"
RETRY_ATTEMPT = "retry.attempt"
RETRY_DELAY_SECONDS = "retry.delay_seconds"

# DLQ/Requeue event names
EVENT_MESSAGE_DEAD_LETTERED = "message.dead_lettered"
EVENT_MESSAGE_REQUEUED = "message.requeued"

# Operation names (used in span names and attributes)
OP_PUBLISH = "publish"
OP_PROCESS = "process"
OP_RECEIVE = "receive"
OP_SETTLE = "settle"
OP_SEND = "send"

# Metric names
METRIC_OPERATION_DURATION = "messaging.client.operation.duration"
METRIC_SENT_MESSAGES = "messaging.client.sent.messages"
METRIC_CONSUMED_MESSAGES = "messaging.client.consumed.messages"
METRIC_CONNECTION_COUNT = "messaging.client.connection.count"
METRIC_RECONNECTIONS = "messaging.client.reconnections"
METRIC_RETRY_ATTEMPTS = "kubemq.client.retry.attempts"
METRIC_RETRY_EXHAUSTED = "kubemq.client.retry.exhausted"
METRIC_SEND_QUEUE_UTILIZATION = "kubemq.send_queue.utilization"

# Histogram bucket boundaries (seconds)
DURATION_HISTOGRAM_BUCKETS = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    30.0,
    60.0,
)

# error.type value mapping (from REQ-ERR-2 categories)
ERROR_TYPE_TRANSIENT = "transient"
ERROR_TYPE_TIMEOUT = "timeout"
ERROR_TYPE_THROTTLING = "throttling"
ERROR_TYPE_AUTHENTICATION = "authentication"
ERROR_TYPE_AUTHORIZATION = "authorization"
ERROR_TYPE_VALIDATION = "validation"
ERROR_TYPE_NOT_FOUND = "not_found"
ERROR_TYPE_FATAL = "fatal"
ERROR_TYPE_CANCELLATION = "cancellation"
ERROR_TYPE_BACKPRESSURE = "backpressure"
