import pandas as pd
import datetime as dt
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator

# Import the necessary classes from different files.
from extract import Extract
from database import Database
from upload import KaggleUpload
from preprocess import Preprocess

# Define the filename for the CSV file.
FILENAME = './dags/data.csv'

# Create a task to extract star wars youtube comments data.
def extract_task():
    # Initialize a list to store comments data.
    comments_data = []
    
    try:
        # Define the data to be extracted.
        df = pd.read_csv(FILENAME)
        
        # Iterate throughout each row of the dataframe.
        for idx, row in df.iterrows():
            star_wars_comment = {
                'comment_id': row['comment_id'],
                'video_title': row['video_title'],
                'author': row['author'],
                'comment': row['comment'],
                'date': row['date'],
                'sentiment': ''
            }
            comments_data.append(star_wars_comment)
            
        # Create an instance of the Extract() object.
        extract = Extract()

        # Get new comments and append them onto this list.
        comments_data = extract.get_comments(comments_data)
        
    except(Exception) as error:
        print("[-] Data failed to extract:", error)
    
    return(comments_data)

# Create a task to preprocess the tweet data.
def clean_task(**context):
    # Create an instance of the Preprocess() object.
    preprocess = Preprocess()
    
    # Use ti.xcom_pull() to pull the returned value of extract_task task from XCom.
    comment_data = context['task_instance'].xcom_pull(task_ids='extract_task')
    
    # Utlize the Preprocess() method to clean data.
    comment_data = preprocess.clean_comment_data(comment_data)
    return(comment_data)

# Create a task to classify the sentiment of each comment within the comment data.
def classify_task(**context):
    # Create an instance of the Preprocess() object.
    preprocess = Preprocess()
    
    # Use ti.xcom_pull() to pull the returned value of clean_task task from XCom.
    comment_data = context['task_instance'].xcom_pull(task_ids='clean_task')
    
    # Classify the sentiments of all comments and add that info to the comment_data.
    comment_data = preprocess.classify_comment_data(comment_data)
    return(comment_data)

# Create a task to allocate comment data to PostgreSQL database table.
def store_task(**context):
    # Create an instance of the Database() object.
    db = Database()
    
    # Use ti.xcom_pull() to pull the returned value of classify_task task from XCom.
    comment_data = context['task_instance'].xcom_pull(task_ids='classify_task')  
    
    # Create the 'Comments' Table within the PostgreSQL database.
    db.create_table()
    # Insert the comment_data into the 'Comments' Table.
    db.store_data(comment_data)
    
    # Write the data to CSV file.
    db.save_into_csv(FILENAME)

# Create a task to upload updated CSV to Kaggle.      
def upload_task():
    try:
        kaggle = KaggleUpload()
        kaggle.upload()
        print('[+] Successfully uploaded to Kaggle')
    except(Exception) as error:
        print('[-] Failed to upload to Kaggle:', error)
    
default_args = {
    'owner': 'Caffeinated Quantum Squadron',
    'start_date': dt.datetime(2023, 4, 27),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# Define the DAG.
dag = DAG(
    dag_id = 'boop111',
    description='YouTube Star Wars Sentiment',
    default_args=default_args,
    schedule_interval='@once'
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
    task_id='store_task',
    python_callable=store_task,
    provide_context=True,
    dag=dag
)
upload_task = PythonOperator(
    task_id='upload_task',
    python_callable=upload_task,
    dag=dag
)

# Specify dependencies between tasks.
extract_task >> clean_task >> classify_task >> store_task >> upload_task

if __name__ == "__main__":
    dag.cli()