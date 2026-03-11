# Commands & Queries API Reference

## Sync Client

::: kubemq.cq.client.Client
    options:
      show_root_heading: true
      members:
        - send_command
        - send_query
        - send_response_message
        - subscribe_to_commands
        - subscribe_to_queries
        - create_commands_channel
        - create_queries_channel
        - delete_commands_channel
        - delete_queries_channel
        - list_commands_channels
        - list_queries_channels
        - ping
        - close

## Async Client

::: kubemq.cq.async_client.AsyncClient

## Command Types

::: kubemq.cq.command_message.CommandMessage

::: kubemq.cq.command_message_received.CommandMessageReceived

::: kubemq.cq.command_response_message.CommandResponseMessage

## Query Types

::: kubemq.cq.query_message.QueryMessage

::: kubemq.cq.query_message_received.QueryMessageReceived

::: kubemq.cq.query_response_message.QueryResponseMessage

## Subscription Types

::: kubemq.cq.commands_subscription.CommandsSubscription

::: kubemq.cq.queries_subscription.QueriesSubscription
