# DatastoreLoadTest
Datastore Load Test

## Running

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