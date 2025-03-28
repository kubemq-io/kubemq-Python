from kubemq.pubsub import Client, EventMessage


def main():
    try:
        client = Client(
            address="localhost:50000",
            client_id="client-id",
            auth_token="eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTY2MTU3NzR9.cfFW3QD6oAFtEh3AFucxTHd7vmt6VIXSWVHTAlIVFnbXJfe6YLybQnjRixKkVa3kgbBFZTo-NcFupU-cdk7vlsSy8as6SRVZ1xWRp59uXgY2P9MorHLv3NFDX7abl6SLZdmSi_PCq5Dakflz13MySxCrD5noEmI6DiWKgRs-4Gxi-linJimt3xgnb34mO5uwh_AK1mQbRf_aO_MdKokss0gT2n-HSGajU45DmIEyFA9zPSzvITu7-mJ1qdFvVo-9JphFlO0nfUvXvJtyn96r_whqAAGt5C1PEoHEx4dp8QTiKbm2Gxf4hsuhH4ShdgUvGCB-UqWVJX4aw3EWxXiAxw",
        )
        client.send_events_message(EventMessage(channel="e1", body=b"hello kubemq"))
        print("Event sent")
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    main()
