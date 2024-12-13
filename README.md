# Data pipeline using Apache Airflow to migrate data from PostgreSQL to Snowflake

![thumbnail](thumbnail.png)

## How to reproduce the project

1. Clone the repository
2. Create a directory `dags` `plugins` `config` `logs` in the root directory
3. Start the docker containers and run `docker compose up airflow-init`
4. When the initialization is done, run `docker compose up` to start the containers
5. When the containers are up and running, go to `localhost:8080` in your browser. this will open the Apache Airflow UI
6. Create a database in PostgreSQL and Snowflake
7. Run the SQL scripts, in this case `articles.sql` in the PostgreSQL database
8. Create a connection in the UI for Snowflake connection and PostgreSQL connection
9. Move the `etl_articles.py` file to the `dags` directory
10. The DAG will be triggered each hour and the data will be migrated from PostgreSQL to Snowflake
