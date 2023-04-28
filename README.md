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
  <img src="https://user-images.githubusercontent.com/96035297/235001126-5f0e5b3e-07af-45f1-8b03-0b303209a3bb.png" height="50" width="50" />
  <img src="https://user-images.githubusercontent.com/96035297/235001916-32b5b3db-8331-43c6-8356-90217de5e45a.svg" height="50" width="50" />
  <img src="https://user-images.githubusercontent.com/96035297/235232871-705f0d65-597d-414f-8120-54410fb848a1.svg" height="50" width="50" />
  <a href="https://skillicons.dev">
    <img src="https://skillicons.dev/icons?i=docker,python,git,vscode" />
  </a>
</p>

## Collaborators
<a href="https://github.com/tyeborg/star-wars-youtube-comments-pipeline/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=tyeborg/star-wars-youtube-comments-pipeline" />
</a>
