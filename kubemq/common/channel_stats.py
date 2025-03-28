import json
from typing import List


class QueuesStats:
    """
    A class that represents statistics for a queue.

    Attributes:
        messages (int): The number of messages in the queue.
        volume (int): The total volume of the messages in the queue.
        waiting (int): The number of messages waiting in the queue.
        expired (int): The number of messages that have expired.
        delayed (int): The number of delayed messages.

    Methods:
        __init__(messages: int, volume: int, waiting: int, expired: int, delayed: int, **kwargs)
            Initializes the QueueStats object with the provided statistics.

        __repr__()
            Returns a string representation of the QueueStats object.
    """

    def __init__(
        self,
        messages: int,
        volume: int,
        waiting: int,
        expired: int,
        delayed: int,
        **kwargs,
    ):
        self.messages = messages
        self.volume = volume
        self.waiting = waiting
        self.expired = expired
        self.delayed = delayed

    def __repr__(self):
        return f"Stats: messages={self.messages}, volume={self.volume}, waiting={self.waiting}, expired={self.expired}, delayed={self.delayed}"


class QueuesChannel:
    """Represents a channel in a queueing system.

    Args:
        name (str): The name of the channel.
        type (str): The type of the channel.
        last_activity (int): The timestamp of the last activity on the channel.
        is_active (bool): Indicates whether the channel is currently active or not.
        incoming (QueuesStats): The statistics of incoming messages on the channel.
        outgoing (QueuesStats): The statistics of outgoing messages on the channel.
        **kwargs: Additional keyword arguments can be provided.

    Attributes:
        name (str): The name of the channel.
        type (str): The type of the channel.
        last_activity (int): The timestamp of the last activity on the channel.
        is_active (bool): Indicates whether the channel is currently active or not.
        incoming (QueuesStats): The statistics of incoming messages on the channel.
        outgoing (QueuesStats): The statistics of outgoing messages on the channel.

    Returns:
        str: A string representation of the QueuesChannel object.

    Example:
        channel = QueuesChannel("channel1", "type1", 1622014799, True, incoming_stats, outgoing_stats)
        print(channel)
    """

    def __init__(
        self,
        name: str,
        type: str,
        last_activity: int,
        is_active: bool,
        incoming: QueuesStats,
        outgoing: QueuesStats,
        **kwargs,
    ):
        self.name = name
        self.type = type
        self.last_activity = last_activity
        self.is_active = is_active
        self.incoming = incoming
        self.outgoing = outgoing

    def __repr__(self):
        return f"Channel: name={self.name}, type={self.type}, last_activity={self.last_activity}, is_active={self.is_active}, incoming={self.incoming}, outgoing={self.outgoing}"


class PubSubStats:
    """
    Initialize the PubSubStats object with the number of messages and volume.

    Args:
        messages (int): The number of messages.
        volume (int): The volume of the messages.

    Returns:
        None
    """

    def __init__(self, messages: int, volume: int, **kwargs):
        self.messages = messages
        self.volume = volume

    def __repr__(self):
        return f"Stats: messages={self.messages}, volume={self.volume}"


class PubSubChannel:
    """
    The PubSubChannel class represents a communication channel used in a publish-subscribe system. It stores information about the channel's name, type, last activity, active status, incoming
    * statistics, and outgoing statistics.

    Attributes:
        name (str): The name of the channel.
        type (str): The type of the channel.
        last_activity (int): The timestamp of the last activity on the channel.
        is_active (bool): Indicates whether the channel is currently active.
        incoming (PubSubStats): The statistics related to incoming messages on the channel.
        outgoing (PubSubStats): The statistics related to outgoing messages on the channel.

    Methods:
        __repr__(): Returns a string representation of the PubSubChannel object.

    Note: The `PubSubStats` class is not defined here, but it is assumed to be a separate class that represents statistics related to message traffic on the channel.
    """

    def __init__(
        self,
        name: str,
        type: str,
        last_activity: int,
        is_active: bool,
        incoming: PubSubStats,
        outgoing: PubSubStats,
        **kwargs,
    ):
        self.name = name
        self.type = type
        self.last_activity = last_activity
        self.is_active = is_active
        self.incoming = incoming
        self.outgoing = outgoing

    def __repr__(self):
        return f"Channel: name={self.name}, type={self.type}, last_activity={self.last_activity}, is_active={self.is_active}, incoming={self.incoming}, outgoing={self.outgoing}"


class CQStats:
    """
    Class representing statistics for a conversation queue.

    Attributes:
        messages (int): The number of messages in the queue.
        volume (int): The volume of the queue.
        responses (int): The number of responses in the queue.

    Methods:
        __init__(messages: int, volume: int, responses: int, **kwargs):
            Initializes a new instance of the CQStats class.
        __repr__():
            Returns a string representation of the CQStats object.

    """

    def __init__(self, messages: int, volume: int, responses: int, **kwargs):
        self.messages = messages
        self.volume = volume
        self.responses = responses

    def __repr__(self):
        return f"Stats: messages={self.messages}, volume={self.volume}, responses={self.responses}"


class CQChannel:
    """

    The CQChannel class represents a channel in a communication system. It stores information about the channel's name, type, last activity timestamp, active status, incoming statistics
    *, and outgoing statistics.

    Attributes:
    - name: A string representing the name of the channel.
    - type: A string representing the type of the channel.
    - last_activity: An integer representing the timestamp of the last activity on the channel.
    - is_active: A boolean indicating whether the channel is active or not.
    - incoming: An instance of the CQStats class representing the incoming statistics of the channel.
    - outgoing: An instance of the CQStats class representing the outgoing statistics of the channel.

    Methods:
    - __init__(self, name: str, type: str, last_activity: int, is_active: bool, incoming: CQStats, outgoing: CQStats, **kwargs):
      Initializes a new instance of the CQChannel class with the specified parameters.

    - __repr__(self):
      Returns a string representation of the CQChannel object.

    """

    def __init__(
        self,
        name: str,
        type: str,
        last_activity: int,
        is_active: bool,
        incoming: CQStats,
        outgoing: CQStats,
        **kwargs,
    ):
        self.name = name
        self.type = type
        self.last_activity = last_activity
        self.is_active = is_active
        self.incoming = incoming
        self.outgoing = outgoing

    def __repr__(self):
        return f"Channel: name={self.name}, type={self.type}, last_activity={self.last_activity}, is_active={self.is_active}, incoming={self.incoming}, outgoing={self.outgoing}"


def decode_pub_sub_channel_list(data_bytes: bytes) -> List[PubSubChannel]:
    """

    Decodes the given data bytes into a list of PubSubChannel objects.

    Parameters:
    - data_bytes (bytes): The data bytes to decode.

    Returns:
    - List[PubSubChannel]: A list of PubSubChannel objects.

    """
    # Decode bytes to string and parse JSON
    data_str = data_bytes.decode("utf-8")
    channels_data = json.loads(data_str)

    channels = []
    for item in channels_data:
        # Extracting incoming and outgoing as Stats objects
        incoming = PubSubStats(**item["incoming"])
        outgoing = PubSubStats(**item["outgoing"])

        # Creating a Channel instance with the Stats objects
        channel = PubSubChannel(
            name=item["name"],
            type=item["type"],
            last_activity=item["lastActivity"],
            is_active=item["isActive"],
            incoming=incoming,
            outgoing=outgoing,
        )
        channels.append(channel)

    return channels


def decode_queues_channel_list(data_bytes: bytes) -> List[QueuesChannel]:
    """
    Decodes a byte string into a list of QueuesChannel objects.

    Parameters:
        - data_bytes (bytes): The byte string to be decoded.

    Returns:
        - List[QueuesChannel]: A list of QueuesChannel objects.

    Note:
        - This method assumes that the byte string is encoded in 'utf-8' format.
        - The byte string should represent a valid JSON object.
        - The JSON object should contain the necessary fields ('name', 'type', 'lastActivity', 'isActive', 'incoming', 'outgoing') for creating QueuesChannel objects.
        - The 'incoming' and 'outgoing' fields should contain valid JSON objects that can be parsed into QueuesStats objects.
    """
    # Decode bytes to string and parse JSON
    data_str = data_bytes.decode("utf-8")
    channels_data = json.loads(data_str)

    channels = []
    for item in channels_data:
        # Extracting incoming and outgoing as Stats objects
        incoming = QueuesStats(**item["incoming"])
        outgoing = QueuesStats(**item["outgoing"])

        # Creating a Channel instance with the Stats objects
        channel = QueuesChannel(
            name=item["name"],
            type=item["type"],
            last_activity=item["lastActivity"],
            is_active=item["isActive"],
            incoming=incoming,
            outgoing=outgoing,
        )
        channels.append(channel)

    return channels


def decode_cq_channel_list(data_bytes: bytes) -> List[CQChannel]:
    """
    Decodes the given byte array into a list of CQChannel objects.

    Parameters:
    - data_bytes (bytes): The byte array to decode.

    Returns:
    - List[CQChannel]: The list of CQChannel objects decoded from the byte array.
    """
    # Decode bytes to string and parse JSON
    data_str = data_bytes.decode("utf-8")
    channels_data = json.loads(data_str)

    channels = []
    for item in channels_data:
        # Extracting incoming and outgoing as Stats objects
        incoming = CQStats(**item["incoming"])
        outgoing = CQStats(**item["outgoing"])

        # Creating a Channel instance with the Stats objects
        channel = CQChannel(
            name=item["name"],
            type=item["type"],
            last_activity=item["lastActivity"],
            is_active=item["isActive"],
            incoming=incoming,
            outgoing=outgoing,
        )
        channels.append(channel)

    return channels
