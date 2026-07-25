import json
import logging
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)

PROJECT_ID = "deloitte-de-home-ty"
TOPIC_ID = "deloitte-user-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

sample_events = [
    {"user_id": "USR_0001", "name": "David", "address_id": "ADDR_0001"},
    {"user_id": "USR_0002", "name": "Sonia", "address_id": "ADDR_0002"},
]

for record in sample_events:
    data_str = json.dumps(record)
    future = publisher.publish(topic_path, data_str.encode("utf-8"))
    logging.info(f"Published message ID: {future.result()}")