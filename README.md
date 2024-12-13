# Data pipeline using Apache Airflow to migrate data from PostgreSQL to Snowflake

![thumbnail](thumbnail.png)

## How to reproduce the project

1. Clone the repository
2. Create a directory `dags` `plugins` `config` `logs` in the root directory
3. Start the docker containers and run `docker compose up airflow-init`
4. When the initialization is done, run `docker compose up`
5. When the containers are up and running, go to `localhost:8080` in your browser
6. Create a connection in the UI for Snowflake connection and PostgreSQL connection
7. Create a database and table `articles` with dummy data in PostgreSQL
8. Move the `etl_articles.py` file to the `dags` directory
9. The DAG will be triggered each hour and the data will be migrated from PostgreSQL to Snowflake
