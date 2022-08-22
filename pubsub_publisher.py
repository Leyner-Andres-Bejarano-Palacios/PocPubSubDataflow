# requirements
# google-cloud-pubsub==1.7.0 
import json
import time
from datetime import datetime
from random import random
from google.auth import jwt
from google.cloud import pubsub_v1

# --- Base variables and auth path
CREDENTIALS_PATH = r"/Users/usuario/Downloads/falabella-356122-a0fc362da30c.json"
PROJECT_ID = "falabella-356122"
TOPIC_ID = "test-topic"
MAX_MESSAGES = 100

# --- PubSub Utils Classes
class PubSubPublisher:
    def __init__(self, credentials_path, project_id, topic_id):
        credentials = jwt.Credentials.from_service_account_info(
            json.load(open(credentials_path)),
            audience="https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        )
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

    def publish(self, data: str):
        result = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        return result


# --- Main publishing script
def main():
    i = 0
    publisher = PubSubPublisher(CREDENTIALS_PATH, PROJECT_ID, TOPIC_ID)
    while i < MAX_MESSAGES:
        data = {
            "attr1": random(),
            "msg": f"Hi-{datetime.now()}"
        }
        publisher.publish(json.dumps(data))
        time.sleep(random())
        i += 1

if __name__ == "__main__":
    main()