# Imports the Google Cloud client library
from google.cloud import datastore
import uuid
from datetime import datetime
from faker import Faker
import os

# Datastore details
PROJECT_ID=os.getenv("PROJECT_ID")
KIND = "Order"
NAMESPACE = "LoadTest"

# Test details
NUMBER_OF_ENTITIES = 100000
COMMIT_SIZE = 500


def load_test():
    
    print("This test will upsert {} records to Datastore".format(NUMBER_OF_ENTITIES))

    # create fake data
    batches = []
    while len(batches) < (NUMBER_OF_ENTITIES/COMMIT_SIZE):
        batches.append(create_fake_entity(COMMIT_SIZE))
    
    # Instantiates a client
    client = datastore.Client(project=PROJECT_ID, namespace=NAMESPACE)

    start_time = datetime.now()
    print(start_time)

    batch_count = 0
    for b in batches:
        batch_count+=1
        tasks = []
        for t in b:
            task = datastore.Entity(client.key(KIND, t['order_id']))
            task.update(t)
            tasks.append(task)
        client.put_multi(tasks)
        print("Batch {} done".format(batch_count))
    
    end_time = datetime.now()
    print(end_time)
    
    delta = end_time - start_time
    print("Time taken to upsert {0} records was {1} seconds".format(NUMBER_OF_ENTITIES, delta.total_seconds()))
    print("Average of {0} records per second".format(NUMBER_OF_ENTITIES/delta.total_seconds()))
    print(f"Done")


def create_fake_entity(num_of_entities):
    fake = Faker()
    entities = []
    for i in range(num_of_entities):
        order = {
            "order_id": str(uuid.uuid1()),
            "customer_email": fake.free_email(),
            "phone_number": fake.phone_number(),
            "user_agent": fake.chrome(),
            "create_time": datetime.now()
        }
        entities.append(order)
    return entities


if __name__ == "__main__":
    load_test()
   