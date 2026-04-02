# PubSub API Reference

## Sync Client

::: kubemq.pubsub.client.Client
    options:
      show_root_heading: true
      members:
        - publish_event
        - publish_event_store
        - subscribe_to_events
        - subscribe_to_events_store
        - create_events_channel
        - create_events_store_channel
        - delete_events_channel
        - delete_events_store_channel
        - list_events_channels
        - list_events_store_channels
        - ping
        - close

## Async Client

::: kubemq.pubsub.async_client.AsyncClient

## Message Types

::: kubemq.pubsub.event_message.EventMessage

::: kubemq.pubsub.event_store_message.EventStoreMessage

::: kubemq.pubsub.event_message_received.EventMessageReceived

::: kubemq.pubsub.event_store_message_received.EventStoreMessageReceived

::: kubemq.pubsub.event_send_result.EventSendResult

## Subscription Types

::: kubemq.pubsub.events_subscription.EventsSubscription

::: kubemq.pubsub.events_store_subscription.EventsStoreSubscription
