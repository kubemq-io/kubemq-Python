from typing import Any

from pydantic import BaseModel, ConfigDict

from kubemq.grpc import Result


class EventSendResult(BaseModel):
    """Result of sending an event message."""

    id: str | None = None
    sent: bool = False
    error: str | None = None

    @classmethod
    def decode(cls, result: Result) -> "EventSendResult":
        """Decode a protobuf Result into an EventSendResult."""
        return cls(id=result.EventID, sent=result.Sent, error=result.Error)

    model_config = ConfigDict(validate_assignment=True)

    def model_dump_json(self, **kwargs: Any) -> str:
        """Serialize the model to JSON, excluding None values."""
        return super().model_dump_json(exclude_none=True, **kwargs)
