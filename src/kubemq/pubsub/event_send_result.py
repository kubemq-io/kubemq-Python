from __future__ import annotations

import json
from dataclasses import dataclass

from kubemq.grpc import Result


@dataclass
class EventStoreResult:
    """Result of sending an event message."""

    id: str | None = None
    sent: bool = False
    error: str | None = None

    @classmethod
    def decode(cls, result: Result) -> EventStoreResult:
        """Decode a protobuf Result into an EventStoreResult."""
        return cls(id=result.EventID, sent=result.Sent, error=result.Error)

    def to_json(self) -> str:
        """Serialize the model to JSON, excluding None values."""
        return json.dumps({k: v for k, v in self.__dict__.items() if v is not None})
