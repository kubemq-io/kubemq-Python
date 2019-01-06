import logging
import threading

from kubemq.basic.grpc_client import GrpcClient
from kubemq.events.event_receive import EventReceive
from kubemq.subscription import SubscribeType, EventsStoreType


class Subscriber(GrpcClient):

    def __init__(self, kubemq_address=None):
        """
        Initialize a new Sender under the requested KubeMQ Server Address.

        :param str kubemq_address: KubeMQ server address. if None will be parsed from Config or environment parameter.
        """
        GrpcClient.__init__(self)
        if kubemq_address:
            self._kubemq_address = kubemq_address

    def subscribe_to_events(self, subscribe_request, handler):

        if not subscribe_request.channel:
            raise ValueError("channel parameter is mandatory.")

        if not subscribe_request.is_valid_type("Events"):
            raise ValueError("Invalid Subscribe Type for this Class.")

        if subscribe_request.subscribe_type == SubscribeType.EventsStore:
            if not subscribe_request.client_id:
                raise ValueError("client_id parameter is mandatory.")
            if subscribe_request.events_store_type == EventsStoreType.Undefined:
                raise ValueError("events_store_type parameter is mandatory.")

        inner_subscribe_request = subscribe_request.to_inner_subscribe_request()

        call = self.get_kubemq_client().SubscribeToEvents(inner_subscribe_request, metadata=self._metadata)

        def subscribe_to_event():
            while True:
                event_receive = call.next()

                logging.info("Subscriber Received Event: EventID:'%s', Channel:'%s', Body:'%s'" % (
                    event_receive.EventID,
                    event_receive.Channel,
                    event_receive.Body
                ))

                handler(EventReceive(event_receive))

        thread = threading.Thread(target=subscribe_to_event, args=())
        thread.daemon = True
        thread.start()
