import csv
import pandas as pd
import datetime as dt
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator

# Import the necessary classes from different files.
from extract import Extract
from database import Database
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
            
        print("Data extracted successfully")
        
    except(Exception) as error:
        print("Data failed to extract:", error)
    
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
    # Insert the tweet_data into the 'Comments' Table.
    db.store_data(comment_data)
    
def save_to_csv_task():
    # Create an instance of the Database() object.
    db= Database()
    
    # Obtain the rows as a list of tuples.
    rows = db.get_data_from_database()
    
    # Write the data to a CSV file.
    with open(FILENAME, mode='w', newline='') as file:
        # Create a CSV writer object. 
        writer = csv.writer(file)
        
        # Initialize the header rows.
        writer.writerow(['comment_id', 'video_title', 'author', 'comment', 'date', 'sentiment'])
        # Insert the rows into the CSV.
        for row in rows:
            writer.writerow(row)
    
#def upload_to_kaggle_task(username, key):
    # Define Kaggle API credentials
    #kaggle_username = 'tylersupersad'
    #kaggle_key = 'b072f733e77f84cfd00d15644c227c3e'
        
    # Create an instance of the Database() object.
    #db = Database()
    
    # Fetch the content from the table into a variable.
    #data = db.get_data()
    #try: 
        # Write the data to a CSV file.
        #with open('./dags/starwars.csv', 'w', newline='') as f:
            #writer = csv.writer(f)
            #writer.writerows(data)
            
        # Upload CSV file to Kaggle dataset.
        #api = KaggleApi()
        #api.authenticate(username=username, key=key)
        #dataset = kaggle.api.dataset_metadata('tylersupersad/star-wars-youtube-comments-sentiment')
        #file_name = './dags/starwars.csv'
        #with open(file_name, 'rb') as f:
            #kaggle.api.dataset_upload_file(dataset['id'], file_name, f)
        
        #print('Successfully uploaded to Kaggle')
            
    #except(Exception) as error:
        #print('Failed to upload to Kaggle:', error)
         
default_args = {
    'owner': 'Caffeinated Quantum Squadron',
    'start_date': dt.datetime(2023, 4, 25),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# Define the DAG.
dag = DAG(
    dag_id = 'boop82',
    description='A pipeline for analyzing Twitter sentiment',
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
    task_id='store_data',
    python_callable=store_task,
    provide_context=True,
    dag=dag
)
save_to_csv_task = PythonOperator(
    task_id='save_to_csv_task',
    python_callable=save_to_csv_task,
    dag=dag
)
#upload_to_kaggle_task = PythonOperator(
    #task_id='upload_to_kaggle',
    #python_callable=upload_to_kaggle_task,
    #op_kwargs={
            #'username': 'tylersupersad',
            #'key': 'b072f733e77f84cfd00d15644c227c3e'
    #},
    #dag=dag
#)

# Specify dependencies between tasks.
extract_task >> clean_task >> classify_task >> store_task >> save_to_csv_task

if __name__ == "__main__":
    dag.cli()