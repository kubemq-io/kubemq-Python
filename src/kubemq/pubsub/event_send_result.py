from typing import Optional

from pydantic import BaseModel, ConfigDict

from kubemq.grpc import Result


class EventSendResult(BaseModel):
    id: Optional[str] = None
    sent: bool = False
    error: Optional[str] = None

    @classmethod
    def decode(cls, result: Result) -> "EventSendResult":
        return cls(id=result.EventID, sent=result.Sent, error=result.Error)

    model_config = ConfigDict(validate_assignment=True)

    def model_dump_json(self, **kwargs):
        return super().model_dump_json(exclude_none=True, **kwargs)
