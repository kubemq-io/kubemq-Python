import logging
import threading
import grpc
from kubemq.grpc import Empty
from kubemq.basic.grpc_client import GrpcClient
from kubemq.events.event_receive import EventReceive
from kubemq.subscription import SubscribeType, EventsStoreType
from kubemq.tools.listener_cancellation_token import ListenerCancellationToken


class Subscriber(GrpcClient):

    def __init__(self, kubemq_address=None, encryptionHeader=None):
        """
        Initialize a new Sender under the requested KubeMQ Server Address.

        :param str kubemq_address: KubeMQ server address. if None will be parsed from Config or environment parameter.
        :param byte[] encryptionHeader: the encrypted header requested by kubemq authentication.
        """
        GrpcClient.__init__(self, encryptionHeader)
        if kubemq_address:
            self._kubemq_address = kubemq_address

    def ping(self):
        """ping check connection to the kubemq"""
        ping_result = self.get_kubemq_client().Ping(Empty())
        logging.debug("event subscriber KubeMQ address:%s ping result:%s'" % (self._kubemq_address, ping_result))
        return ping_result

    def subscribe_to_events(self, subscribe_request, handler, error_handler,
                            listener_cancellation_token=ListenerCancellationToken()):
        """
        Register to kubeMQ Channel using handler.
        :param SubscribeRequest subscribe_request: represent by that will determine the subscription configuration.
        :param handler: Method the perform when receiving EventReceive
        :param error_handler: Method the perform when receiving error from kubemq
        :param listener_cancellation_token: cancellation token, once cancel is called will cancel the subscribe to kubemq
        :return: A thread running the Subscribe Request.
        """

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

        def subscribe_to_event(listener_cancellation_token):
            try:
                while True:
                    event_receive = call.next()

                    logging.info("Subscriber Received Event: EventID:'%s', Channel:'%s', Body:'%s Tags:%s'" % (
                        event_receive.EventID,
                        event_receive.Channel,
                        event_receive.Body,
                        event_receive.Tags
                    ))

                    handler(EventReceive(event_receive))
            except grpc.RpcError as error:
                if (listener_cancellation_token.is_cancelled):
                    logging.info("Sub closed by listener request")
                    error_handler(str(error))
                else:
                    logging.exception("Subscriber Received Error: Error:'%s'" % (error))
                    error_handler(str(error))
            except Exception as e:
                logging.exception("Subscriber Received Error: Error:'%s'" % (e))
                error_handler(str(e))

        def check_sub_to_valid(listener_cancellation_token):
            while True:
                if (listener_cancellation_token.is_cancelled):
                    logging.info("Sub closed by listener request")
                    call.cancel()
                    return

        thread = threading.Thread(target=subscribe_to_event, args=(listener_cancellation_token,))
        thread.daemon = True
        thread.start()

        listener_thread = threading.Thread(target=check_sub_to_valid, args=(listener_cancellation_token,))
        listener_thread.daemon = True
        listener_thread.start()
        return thread
