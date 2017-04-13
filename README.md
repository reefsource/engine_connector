This mongo connector is designed to listen for new jobs (new record in the mongo collection `jobs`).
When a new job is found, it runs a docker container to run an analysis on the input file, and uploads the results back to the API.


startup from the root folder
```
PYTHONPATH=. nohup mongo-connector -m localhost:27017 -d engine_simulator -n 'scitran-core.jobs' > ../mongo-connector.log &
```
options
`-m`: address of the mongo db instance
`-d`: docmanager used, in our case the engine_simulator defined in this github repo
`-n`: collections on which we are listening

We are only listening on the `jobs` collection in the db `scitran-core`.

Also, as this is a prototype, the `run_job` method used in the engine_simulator doc manager runs always the same docker container.
However it should be possible by modifying that method to check the type of job and run a different container.
