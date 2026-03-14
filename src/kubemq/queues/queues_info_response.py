from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from kubemq.grpc import kubemq_pb2 as pb


class QueueInfoModel(BaseModel):
    """Per-queue statistics returned by the ``QueuesInfo`` RPC.

    Attributes:
        name: Queue channel name.
        messages: Total messages currently in the queue.
        bytes: Total size of all messages in the queue (bytes).
        first_sequence: Sequence number of the oldest message.
        last_sequence: Sequence number of the newest message.
        sent: Cumulative messages sent (published) to this queue.
        delivered: Cumulative messages delivered (consumed) from this queue.
        waiting: Messages waiting to be consumed.
        subscribers: Number of active subscribers.
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(default="")
    messages: int = Field(default=0)
    bytes: int = Field(default=0)
    first_sequence: int = Field(default=0)
    last_sequence: int = Field(default=0)
    sent: int = Field(default=0)
    delivered: int = Field(default=0)
    waiting: int = Field(default=0)
    subscribers: int = Field(default=0)

    @classmethod
    def decode(cls, info: pb.QueueInfo) -> QueueInfoModel:
        return cls(
            name=info.Name,
            messages=info.Messages,
            bytes=info.Bytes,
            first_sequence=info.FirstSequence,
            last_sequence=info.LastSequence,
            sent=info.Sent,
            delivered=info.Delivered,
            waiting=info.Waiting,
            subscribers=info.Subscribers,
        )

    def __repr__(self) -> str:
        return (
            f"QueueInfoModel(name={self.name!r}, messages={self.messages}, "
            f"bytes={self.bytes}, first_sequence={self.first_sequence}, "
            f"last_sequence={self.last_sequence}, sent={self.sent}, "
            f"delivered={self.delivered}, waiting={self.waiting}, "
            f"subscribers={self.subscribers})"
        )


class QueuesInfoModel(BaseModel):
    """Aggregate queue statistics returned by the ``QueuesInfo`` RPC.

    Attributes:
        ref_request_id: Correlation ID echoed from the request.
        total_queues: Total number of queues matching the filter.
        sent: Aggregate messages sent across all matching queues.
        delivered: Aggregate messages delivered across all matching queues.
        waiting: Aggregate messages waiting across all matching queues.
        queues: Per-queue breakdown.
    """

    model_config = ConfigDict(frozen=True)

    ref_request_id: str = Field(default="")
    total_queues: int = Field(default=0)
    sent: int = Field(default=0)
    delivered: int = Field(default=0)
    waiting: int = Field(default=0)
    queues: list[QueueInfoModel] = Field(default_factory=list)

    @classmethod
    def decode(cls, response: pb.QueuesInfoResponse) -> QueuesInfoModel:
        info = response.Info
        return cls(
            ref_request_id=response.RefRequestID,
            total_queues=info.TotalQueue if info else 0,
            sent=info.Sent if info else 0,
            delivered=info.Delivered if info else 0,
            waiting=info.Waiting if info else 0,
            queues=[QueueInfoModel.decode(q) for q in info.Queues] if info else [],
        )

    def __repr__(self) -> str:
        return (
            f"QueuesInfoModel(total_queues={self.total_queues}, "
            f"sent={self.sent}, delivered={self.delivered}, "
            f"waiting={self.waiting}, queues={len(self.queues)} items)"
        )

    def __len__(self) -> int:
        return len(self.queues)

    def __iter__(self):
        return iter(self.queues)

    def __getitem__(self, index):
        return self.queues[index]
