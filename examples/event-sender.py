import datetime

from kubemq.events.lowlevel.event import Event
from kubemq.events.lowlevel.sender import Sender

if __name__ == "__main__":
    print("Sending event using sender example")

    sender = Sender("localhost:50000")
    event = Event(
        metadata="EventMetaData",
        body=("Event Created on time %s" % datetime.datetime.utcnow()).encode('UTF-8'),
        store=False,
        channel="MyTestChannelName",
        client_id="EventSender"
    )
    event.tags=[
            ('key', 'value'),
            ('key2', 'value2'),
        ]
    sender.send_event(event)
