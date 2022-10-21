# Imports the Google Cloud client library
from google.cloud import datastore
import uuid
from datetime import datetime
from faker import Faker
import os
import concurrent.futures


# Datastore details
PROJECT_ID=os.getenv("PROJECT_ID")
KIND = "Order"
NAMESPACE = "LoadTest"

# Test details
NUMBER_OF_ENTITIES = 50000
COMMIT_SIZE = 500


def load_test():
    
    print("This script will upsert {} records to Datastore".format(NUMBER_OF_ENTITIES))

    # create fake data
    print("Creating batches with fake data...")
    batches = []
    while len(batches) < (NUMBER_OF_ENTITIES/COMMIT_SIZE):
        batches.append(create_fake_entity(COMMIT_SIZE))
    
    print("Loading data to Datastore...")

    start_time = datetime.now()
    print("Start time {}".format(start_time))

    poolBatches(batches)
    
    end_time = datetime.now()
    print("End time {}".format(end_time))
    
    delta = end_time - start_time
    print("Time taken to upsert {0} records was {1} seconds".format(NUMBER_OF_ENTITIES, delta.total_seconds()))
    print("Average of {0} records per second".format(NUMBER_OF_ENTITIES/delta.total_seconds()))
    print(f"Done")


def poolBatches(batches):
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(load_batch_to_datastore, batches)


def load_batch_to_datastore(batch):
    client = datastore.Client(project=PROJECT_ID, namespace=NAMESPACE)
    upsert_list = []
    for o in batch:
        entity = datastore.Entity(client.key(KIND, o['order_id']))
        entity.update(o)
        upsert_list.append(entity)
    client.put_multi(upsert_list)
    print("Upsert done!")


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
   