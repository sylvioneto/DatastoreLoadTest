# Imports the Google Cloud client library
from google.cloud import datastore
import uuid
from datetime import datetime, timedelta
from faker import Faker
import os
import concurrent.futures


# Datastore details
KIND = "MyOrders"
NAMESPACE = "LoadTest"

# Test details
NUMBER_OF_ENTITIES = 10000
COMMIT_SIZE = 500

PROJECT_ID = os.getenv("PROJECT_ID")
if not PROJECT_ID:
    raise Exception("PROJECT_ID not set, please this env var.")
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f+%z"

client = datastore.Client(project=PROJECT_ID, namespace=NAMESPACE)

# Start the test
def load_test():
    print("This script will upsert {} records to Datastore".format(NUMBER_OF_ENTITIES))

    print("Creating batches of entities with fake data...")
    batches_of_entities = []
    while len(batches_of_entities) < (NUMBER_OF_ENTITIES/COMMIT_SIZE):
        batches_of_entities.append(create_fake_entities(COMMIT_SIZE))

    print("Loading data into Datastore...")

    start_time = datetime.now()
    print("Start time {}".format(start_time))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(insertEntities, batches_of_entities)

    end_time = datetime.now()
    print("End time {}".format(end_time))

    delta = end_time - start_time
    print("Time taken to upsert {0} records was {1} seconds".format(NUMBER_OF_ENTITIES, delta.total_seconds()))
    print("Average of {0} records per second".format(NUMBER_OF_ENTITIES/delta.total_seconds()))
    print(f"Done")


def insertEntities(entities):
    client.put_multi(entities)


# return fake data for testing
def create_fake_entities(num_of_entities):   
    fake = Faker()
    entities = []
    for i in range(num_of_entities):
        doc_key = str(uuid.uuid4())
        entity = datastore.Entity(client.key(KIND, doc_key))
        entity.update({
            "customerEmail": fake.free_email(),
            "phoneNumber": fake.phone_number(),
            "userAgent": fake.chrome(),
            "createAt": datetime.now().strftime(DATETIME_FORMAT),
            "expireAt": (datetime.now() + timedelta(days=3)).strftime(DATETIME_FORMAT)
        })
        entities.append(entity)
    return entities


if __name__ == "__main__":
    load_test()
