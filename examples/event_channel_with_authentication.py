import jwt
from kubemq.events.channel import Channel
from kubemq.events.channel_parameters import ChannelParameters
from kubemq.events.event import Event

if __name__ == "__main__":
    print("Sending event on channel example")
    params = ChannelParameters(
        channel_name="MyTestChannelName",
        client_id="EventChannelID",
        store=True,
        return_result=False,
        kubemq_address="localhost:50000",
        encryptionHeader=jwt.encode({}, algorithm="HS256", key="some-key")
    )

    channel = Channel(params=params)

    event = Event(body="Event".encode('UTF-8'), metadata="EventChannel")

    try:
        channel.send_event(event)
    except Exception as err:
        print('error, error:%s' % (
            err
        ))