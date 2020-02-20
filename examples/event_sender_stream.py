import datetime

from kubemq.events.lowlevel.event import Event
from kubemq.events.lowlevel.sender import Sender

if __name__ == "__main__":
    print("Sending event using sender example")

    sender = Sender("localhost:50000")


    def async_streamer():
        for counter in range(3):
            yield Event(
                metadata="EventMetaData",
                body=("Event %s Created on time %s" % (counter, datetime.datetime.utcnow())).encode('UTF-8'),
                store=False,
                channel="MyTestChannelName",
                client_id="EventSender",
            )


    def result_handler(result):
        print(result)


    sender.stream_event(async_streamer(), result_handler)
