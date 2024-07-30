from typing import Optional
from pydantic import BaseModel
from kubemq.grpc import Result


class EventSendResult(BaseModel):
    id: Optional[str] = None
    sent: bool = False
    error: Optional[str] = None

    @classmethod
    def decode(cls, result: Result) -> "EventSendResult":
        return cls(id=result.EventID, sent=result.Sent, error=result.Error)

    def model_dump_json(self, **kwargs):
        return super().model_dump_json(exclude_none=True, **kwargs)

    class Config:
        validate_assignment = True
