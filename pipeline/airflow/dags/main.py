import re
import pandas as pd
import datetime as dt
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator

# Import the necessary classes from different files.
from database import Database
from preprocess import Preprocess

# Create a task to extract tweet data.
def extract_task():
    # Initialize a list to store tweet data.
    tweet_data = []
    try:
        # Define the data to be extracted.
        df = pd.read_csv('./dags/data.csv')
        
        # Iterate throughout each row of the dataframe.
        for idx, row in df.iterrows():
            tweet = {
                'id': row['id'],
                'created_at': row['date'],
                'text': row['text'],
                'user': row['user'],
                'sentiment': ''
            }
            tweet_data.append(tweet)
            
        print("Data extracted successfully")
        
    except(Exception) as error:
        print("Data failed to extract:", error)
        
    return(tweet_data)

# Create a task to preprocess the tweet data.
def clean_task(**context):
    # Create an instance of the Preprocess() object.
    preprocess = Preprocess()
    
    # Use ti.xcom_pull() to pull the returned value of extract_task task from XCom.
    tweet_data = context['task_instance'].xcom_pull(task_ids='extract_task')
    
    # Utlize the Preprocess() method to clean data.
    tweet_data = preprocess.clean_tweet_data(tweet_data)
    return(tweet_data)

# Create a task to classify the sentiment of each tweet within the tweet data.
def classify_task(**context):
    # Create an instance of the Preprocess() object.
    preprocess = Preprocess()
    
    # Use ti.xcom_pull() to pull the returned value of extract_task task from XCom.
    tweet_data = context['task_instance'].xcom_pull(task_ids='clean_task')
    
    # Classify the sentiments of all tweets and add that info to the tweet_data.
    tweet_data = preprocess.classify_tweet_data(tweet_data)
    return(tweet_data)
    
# Create a task to allocate tweet data to PostgreSQL database table.
def store_task(**context):
    # Create an instance of the Database() object.
    db = Database()
    
    # Use ti.xcom_pull() to pull the returned value of clean_task task from XCom.
    tweet_data = context['task_instance'].xcom_pull(task_ids='classify_task')  
    
    # Create the 'Tweets' Table within the PostgreSQL database.
    db.create_table()
    # Insert the tweet_data into the 'Tweets' Table.
    db.store_data(tweet_data)
    
default_args = {
    'owner': 'Caffeinated Quantum Squadron',
    'start_date': dt.datetime(2023, 4, 23),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# Define the DAG.
dag = DAG(
    dag_id = 'boop77',
    description='A pipeline for analyzing Twitter sentiment',
    default_args=default_args,
    schedule='@once'
)
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    provide_context=True,
    dag=dag
)
clean_task = PythonOperator(
    task_id='clean_task',
    python_callable=clean_task,
    provide_context=True,
    dag=dag
)
classify_task = PythonOperator(
    task_id='classify_task',
    python_callable=classify_task,
    provide_context=True,
    dag=dag
)
store_task = PythonOperator(
    task_id='store_data',
    python_callable=store_task,
    provide_context=True,
    dag=dag
)

# Specify dependencies between tasks.
extract_task >> clean_task >> classify_task >> store_task

if __name__ == "__main__":
    dag.cli()