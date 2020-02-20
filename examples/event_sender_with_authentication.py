import datetime
import jwt
import sys
sys.path.append(".")
from kubemq.events.lowlevel.event import Event
from kubemq.events.lowlevel.sender import Sender

if __name__ == "__main__":
    print("Sending event using sender example")
    encryptionHeader = jwt.encode({},algorithm="HS256",key="some-key")
    sender = Sender("localhost:50000",encryptionHeader)
    event = Event(
        metadata="EventMetaData",
        body=("Event Created on time %s" % datetime.datetime.utcnow()).encode('UTF-8'),
        store=True,
        channel="MyTestChannelName",
        client_id="EventSender"
    )
    event.tags=[
            ('key', 'value'),
            ('key2', 'value2'),
        ]
    sender.send_event(event)
