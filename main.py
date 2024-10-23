import asyncio
import json
import os
import psutil
import time
import uuid
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = "<your_eventhub_name>"

async def send_event(producer):
    while True:
        # Gather system metrics
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        cpu = psutil.cpu_percent()
        mem = psutil.virtual_memory().percent

        # Prepare the data to send
        data = {
            "id": str(uuid.uuid4()),
            "system": "home",  # Change this to create data for another system if needed
            "timestamp": timestamp,
            "cpu_usage": cpu,
            "mem_usage": mem,
        }

        # Create a batch of events to send
        event_data_batch = await producer.create_batch()

        # Add events to the batch
        event_data_batch.add(EventData(json.dumps(data)))

        # Send the batch of events to the event hub
        await producer.send_batch(event_data_batch)

        # Print the message sent for logging purposes
        print(f"Sent event: {data}")

        # Wait for 5 seconds before sending the next event
        await asyncio.sleep(5)


async def run():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    async with producer:
        await send_event(producer)


if __name__ == "__main__":
    asyncio.run(run())
