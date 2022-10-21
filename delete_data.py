# Imports the Google Cloud client library
from google.cloud import datastore
import os

# Datastore details
PROJECT_ID=os.getenv("PROJECT_ID")
KIND = "Order"
NAMESPACE = "LoadTest"

print("This script delete the test data")
    
# Instantiates a client
client = datastore.Client(project=PROJECT_ID, namespace=NAMESPACE)

query = client.query(kind=KIND)
query.keys_only()
keys = query.fetch()

delete_batch = []
batch_count = 0
keys_count = 0

for k in keys:
    delete_batch.append(k)
    keys_count+=1
    if keys_count % 500 == 0:
        batch_count+=1
        print("Deleting keys... Batch {}".format(batch_count))
        client.delete_multi(delete_batch)
        delete_batch = []
    client.delete_multi(delete_batch)

print(f"Done")

