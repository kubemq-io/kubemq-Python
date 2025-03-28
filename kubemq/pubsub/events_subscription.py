from typing import Callable, Optional
from pydantic import BaseModel, field_validator
from kubemq.grpc import Subscribe
from kubemq.common.subscribe_type import SubscribeType
from kubemq.pubsub import EventMessageReceived


class EventsSubscription(BaseModel):
    channel: str
    group: Optional[str] = None
    on_receive_event_callback: Callable[[EventMessageReceived], None]
    on_error_callback: Optional[Callable[[str], None]] = None

    @field_validator("channel")
    def channel_must_exist(cls, v):
        if not v:
            raise ValueError("Event subscription must have a channel.")
        return v

    def raise_on_receive_message(self, received_event: EventMessageReceived):
        if self.on_receive_event_callback:
            self.on_receive_event_callback(received_event)

    def raise_on_error(self, msg: str):
        if self.on_error_callback:
            self.on_error_callback(msg)

    def encode(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group or ""
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Events.value
        return request

    class Config:
        arbitrary_types_allowed = True

    def model_dump(self, **kwargs):
        dump = super().model_dump(**kwargs)
        # Remove callback functions from the dump
        dump.pop("on_receive_event_callback", None)
        dump.pop("on_error_callback", None)
        return dump
