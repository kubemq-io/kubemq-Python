# Queues API Reference

## Sync Client

::: kubemq.queues.client.Client
    options:
      show_root_heading: true
      members:
        - send_queue_message
        - receive_queue_messages
        - waiting
        - pull
        - create_queues_channel
        - delete_queues_channel
        - list_queues_channels
        - ping
        - close

## Async Client

::: kubemq.queues.async_client.AsyncClient

## Message Types

::: kubemq.queues.queues_message.QueueMessage

::: kubemq.queues.queues_message_received.QueueMessageReceived

::: kubemq.queues.queues_send_result.QueueSendResult

::: kubemq.queues.queues_poll_response.QueuesPollResponse
