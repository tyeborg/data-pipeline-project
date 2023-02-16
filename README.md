# Mean Tweet Pipeline

## Installation
(1) Clone this repository by `git clone https://github.com/tyeborg/mean-tweet-pipeline.git`

(2) Navigate to the `pipeline` directory by entering the following in the command line: 
```bash
cd pipeline
```
(3) Then, navigate to the `airflow` directory by entering the following in the command line:
```bash
cd airflow
```

(4) Open the Docker Application and ensure that you don't have any other containers running using `docker ps`

(5) On all operating systems, you need to run database migrations and create the first user account. To do this, run:
```bash
docker compose up airflow-init
```

(6) After initialization is complete, enter the following to build the Docker container (running Airflow services):
```bash
docker compose up
```
(7) Visit Apache Airflow webserver at: `http://localhost:8080`

## Languages & Tools Utilized

<p float="left">
  <a href="https://skillicons.dev">
    <img src="https://skillicons.dev/icons?i=docker,python,twitter,git,vscode" />
  </a>
</p>
