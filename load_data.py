# Imports the Google Cloud client library
from google.cloud import datastore
import uuid
from datetime import datetime
from faker import Faker
import os
import concurrent.futures


# Datastore details
PROJECT_ID=os.getenv("PROJECT_ID")
if not PROJECT_ID:
    raise Exception("PROJECT_ID not set, please this env var.")
KIND = "Order"
NAMESPACE = "LoadTest"

# Test details
NUMBER_OF_ENTITIES = 50000
COMMIT_SIZE = 500


# Start the test
def load_test():
    print("This script will upsert {} records to Datastore".format(NUMBER_OF_ENTITIES))

    print("Creating batches of entities with fake data...")
    batches = []
    while len(batches) < (NUMBER_OF_ENTITIES/COMMIT_SIZE):
        batches.append(create_fake_entities(COMMIT_SIZE))
    
    print("Loading data to Datastore...")

    start_time = datetime.now()
    print("Start time {}".format(start_time))
    processBatches(batches)
    end_time = datetime.now()
    print("End time {}".format(end_time))
    
    delta = end_time - start_time
    print("Time taken to upsert {0} records was {1} seconds".format(NUMBER_OF_ENTITIES, delta.total_seconds()))
    print("Average of {0} records per second".format(NUMBER_OF_ENTITIES/delta.total_seconds()))
    print(f"Done")


# split batches in the pool
def processBatches(batches):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(load_entities_to_datastore, batches)


# load entities to Datastore
def load_entities_to_datastore(entities):
    client = datastore.Client(project=PROJECT_ID, namespace=NAMESPACE)
    upsert_list = []
    for e in entities:
        entity = datastore.Entity(client.key(KIND, e['order_id']))
        entity.update(e)
        upsert_list.append(entity)
    client.put_multi(upsert_list)


# return fake data for testing
def create_fake_entities(num_of_entities):
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
   