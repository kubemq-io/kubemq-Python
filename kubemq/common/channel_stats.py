import json
from typing import List
class QueuesStats:
    def __init__(self, messages: int, volume: int, waiting:int, expired: int, delayed: int, **kwargs):
        self.messages = messages
        self.volume = volume
        self.waiting=waiting
        self.expired=expired
        self.delayed=delayed
    def __repr__(self):
        return f"Stats: messages={self.messages}, volume={self.volume}, waiting={self.waiting}, expired={self.expired}, delayed={self.delayed}"

class QueuesChannel:
    def __init__(self, name: str, type: str, last_activity: int, is_active: bool, incoming: QueuesStats, outgoing: QueuesStats,**kwargs):
        self.name = name
        self.type = type
        self.last_activity = last_activity
        self.is_active = is_active
        self.incoming = incoming
        self.outgoing = outgoing
    def __repr__(self):
        return f"Channel: name={self.name}, type={self.type}, last_activity={self.last_activity}, is_active={self.is_active}, incoming={self.incoming}, outgoing={self.outgoing}"

class PubSubStats:
    def __init__(self, messages: int, volume: int,**kwargs):
        self.messages = messages
        self.volume = volume
    def __repr__(self):
        return f"Stats: messages={self.messages}, volume={self.volume}"

class PubSubChannel:
    def __init__(self, name: str, type: str, last_activity: int, is_active: bool, incoming: PubSubStats, outgoing: PubSubStats,**kwargs):
        self.name = name
        self.type = type
        self.last_activity = last_activity
        self.is_active = is_active
        self.incoming = incoming
        self.outgoing = outgoing
    def __repr__(self):
        return f"Channel: name={self.name}, type={self.type}, last_activity={self.last_activity}, is_active={self.is_active}, incoming={self.incoming}, outgoing={self.outgoing}"

class CQStats:
    def __init__(self, messages: int, volume: int, responses: int, **kwargs):
        self.messages = messages
        self.volume = volume
        self.responses = responses
    def __repr__(self):
        return f"Stats: messages={self.messages}, volume={self.volume}, responses={self.responses}"

class CQChannel:
    def __init__(self, name: str, type: str, last_activity: int, is_active: bool, incoming: CQStats, outgoing: CQStats,**kwargs):
        self.name = name
        self.type = type
        self.last_activity = last_activity
        self.is_active = is_active
        self.incoming = incoming
        self.outgoing = outgoing
    def __repr__(self):
        return f"Channel: name={self.name}, type={self.type}, last_activity={self.last_activity}, is_active={self.is_active}, incoming={self.incoming}, outgoing={self.outgoing}"

def decode_pub_sub_channel_list(data_bytes: bytes) -> List[PubSubChannel]:
    # Decode bytes to string and parse JSON
    data_str = data_bytes.decode('utf-8')
    channels_data = json.loads(data_str)

    channels = []
    for item in channels_data:
        # Extracting incoming and outgoing as Stats objects
        incoming = PubSubStats(**item['incoming'])
        outgoing = PubSubStats(**item['outgoing'])

        # Creating a Channel instance with the Stats objects
        channel = PubSubChannel(name=item['name'], type=item['type'], last_activity=item['lastActivity'],
                          is_active=item['isActive'], incoming=incoming, outgoing=outgoing)
        channels.append(channel)

    return channels
def decode_queues_channel_list(data_bytes: bytes) -> List[QueuesChannel]:
    # Decode bytes to string and parse JSON
    data_str = data_bytes.decode('utf-8')
    channels_data = json.loads(data_str)

    channels = []
    for item in channels_data:
        # Extracting incoming and outgoing as Stats objects
        incoming = QueuesStats(**item['incoming'])
        outgoing = QueuesStats(**item['outgoing'])

        # Creating a Channel instance with the Stats objects
        channel = QueuesChannel(name=item['name'], type=item['type'], last_activity=item['lastActivity'],
                          is_active=item['isActive'], incoming=incoming, outgoing=outgoing)
        channels.append(channel)

    return channels


def decode_cq_channel_list(data_bytes: bytes) -> List[CQChannel]:
    # Decode bytes to string and parse JSON
    data_str = data_bytes.decode('utf-8')
    channels_data = json.loads(data_str)

    channels = []
    for item in channels_data:
        # Extracting incoming and outgoing as Stats objects
        incoming = CQStats(**item['incoming'])
        outgoing = CQStats(**item['outgoing'])

        # Creating a Channel instance with the Stats objects
        channel = CQChannel(name=item['name'], type=item['type'], last_activity=item['lastActivity'],
                          is_active=item['isActive'], incoming=incoming, outgoing=outgoing)
        channels.append(channel)

    return channels