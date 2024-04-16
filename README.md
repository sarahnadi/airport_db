This is now automated in the CI
```
docker build -t dbinit:0.1 .
docker run --env-file .env dbinit:0.1
```
to run DAG
```
# CSV to local airports.db then to remote (not the optimal way)
dagster dev -f git_csv_turso_init.py
or
# from CSV directly to remote database (optimal way)
dagster dev -f git_csv_sqlalchemy.py 
```
