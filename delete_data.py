# Imports the Google Cloud client library
from google.cloud import datastore
import os

# Datastore details
KIND = "stress"
NAMESPACE = "colmeia"
LIMIT=500

PROJECT_ID=os.getenv("PROJECT_ID")
if not PROJECT_ID:
    raise Exception("PROJECT_ID not set, please this env var.")

print("This script delete the test data")

# Instantiates a client
client = datastore.Client(project=PROJECT_ID, namespace=NAMESPACE)

query = client.query(kind=KIND)
query.keys_only()
entities_to_delete = list(query.fetch(limit=LIMIT))

delete_count = 1
while len(entities_to_delete) > 0:
    print("Deleting keys... Batch {}".format(delete_count))
    client.delete_multi(entities_to_delete)
    entities_to_delete = list(query.fetch(limit=LIMIT))
    delete_count+=1

print(f"Done")

