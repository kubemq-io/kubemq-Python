import asyncio
from kubemq.config.connection import Connection
from kubemq.config.tls_config import TlsConfig
from kubemq.transport._transport import Transport
from kubemq.transport.server_info import ServerInfo
conn = Connection(
    address="localhost:50000",
    client_id="test-connection-client-id",
    tls= TlsConfig(
        ca_file="general/rootCA.pem",
        cert_file="general/localhost.pem",
        key_file="general/localhost-key.pem",
        enabled=True,
    ),
)


async def main():
    # Initialize your Transport object
    transport = Transport(conn)
    await transport._initialize_async()

    result: ServerInfo = await transport._ping_async()

    # Use the result as needed
    print(result)


if __name__ == "__main__":
    asyncio.run(main())

