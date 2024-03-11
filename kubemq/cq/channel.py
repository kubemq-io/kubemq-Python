import json
from typing import List
class Stats:
    def __init__(self, messages: int, volume: int, responses: int, **kwargs):
        self.messages = messages
        self.volume = volume
        self.responses = responses
    def __repr__(self):
        return f"Stats: messages={self.messages}, volume={self.volume}, responses={self.responses}"

class Channel:
    def __init__(self, name: str, type: str, last_activity: int, is_active: bool, incoming: Stats, outgoing: Stats,**kwargs):
        self.name = name
        self.type = type
        self.last_activity = last_activity
        self.is_active = is_active
        self.incoming = incoming
        self.outgoing = outgoing
    def __repr__(self):
        return f"Channel: name={self.name}, type={self.type}, last_activity={self.last_activity}, is_active={self.is_active}, incoming={self.incoming}, outgoing={self.outgoing}"

def decode_channel_list(data_bytes: bytes) -> List[Channel]:
    # Decode bytes to string and parse JSON
    data_str = data_bytes.decode('utf-8')
    channels_data = json.loads(data_str)

    channels = []
    for item in channels_data:
        # Extracting incoming and outgoing as Stats objects
        incoming = Stats(**item['incoming'])
        outgoing = Stats(**item['outgoing'])

        # Creating a Channel instance with the Stats objects
        channel = Channel(name=item['name'], type=item['type'], last_activity=item['lastActivity'],
                          is_active=item['isActive'], incoming=incoming, outgoing=outgoing)
        channels.append(channel)

    return channels