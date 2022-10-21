# DatastoreLoadTest
Datastore Load Test

## Running

### Pre-req
If you are running locally, please ensure you have gcloud installed and run gcloud auth application-default login in order to set the default credentials.

Alternatively, you can set GOOGLE_APPLICATION_CREDENTIALS.

### Prepare the environment
```
python3 -m virtualenv venv
source venv/bin/activate
pip install --upgrade -r requirements.txt

export PROJECT_ID=<YOUR PROJECT ID>
```

### Run the load test
```
python load_data.py
```

### Delete the loaded data
```
python delete_date.py
```
