# Imports the Google Cloud client library
from google.cloud import datastore


# Datastore details
PROJECT_ID="syl-sandbox"
KIND = "Order"
NAMESPACE = "LoadTest"

    
# Instantiates a client
client = datastore.Client(project=PROJECT_ID, namespace=NAMESPACE)

query = client.query(kind=KIND)
query.keys_only()
batch_count = 0

keys = query.fetch()

keys_to_delete = []

while keys.max_results is not None:
    keys_to_delete.append(keys.__next__)
    if i+1 % 500:
        client.delete_multi(keys_to_delete)
        batch_count+=1
        print("Deleting keys... Batch {}".format(batch_count))
        keys_to_delete = []


print(f"Done")

