# Star Wars YouTube Comments Sentiment Pipeline

## Installation
1). Clone this repository by `git clone https://github.com/tyeborg/mean-tweet-pipeline.git`

2). Navigate to the `pipeline` directory by entering the following in the command line: 
```bash
cd pipeline
```
3). Then, navigate to the `airflow` directory by entering the following in the command line:
```bash
cd airflow
```
4). Install the required packages to execute the pipeline:
```bash
pip install -r requirements.txt
```
5). Open the Docker Application and ensure that you don't have any other containers running using `docker ps`

6). On all operating systems, you need to run database migrations and create the first user account. To do this, run:
```bash
docker compose up airflow-init
```

7). After initialization is complete, enter the following to build the Docker container (running Airflow services):
```bash
docker compose up
```
8). Visit Apache Airflow webserver at: `http://localhost:8080`

9). Login into Airflow -- Login: `airflow`; Password: `airflow`

10). Navigate to the `Admin` tab and select `Connections` to establish a connection to the PostgreSQL database.

11). Create a connection with the following information:
* Connection Id: `postgre_sql`
* Connection Type: `Postgres`
* Host: `postgres`
* Schema: `airflow`
* Login: `airflow`
* Password: `airflow`
* Port: `5432`

12). Navigate to the `Admin` tab once more and select `Variables` to create a variable that enables access towards the established PostgreSQL database.

13). Create a variable with the following information:
* Key: `postgres_connection`
* Val: 
```bash
{
    "host": "postgres",
    "port": "5432",
    "schema": "airflow",
    "login": "airflow",
    "password": "airflow"
}
```

## Languages & Tools Utilized

<p float="left">
  <a href="https://skillicons.dev">
    <img src="https://skillicons.dev/icons?i=docker,python,kaggle,youtube,git,vscode" />
  </a>
</p>
