from enum import Enum


class SubscribeType(Enum):
    """Type of subscription operation pattern"""

    SubscribeTypeUndefined = 0
    """Default"""

    Events = 1
    """PubSub event"""

    EventsStore = 2
    """PubSub event with persistence"""

    Commands = 3
    """ReqRep perform action"""

    Queries = 4
    """ReqRep return data"""
