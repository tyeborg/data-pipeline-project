# Import the appropriate libraries for use.
import re
import time
import nltk
import string
import nltk.corpus
import pandas as pd
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

import psycopg2
import datetime as dt
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def fill_database():
    cur = None
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow")
        cur = conn.cursor()
        create_command = """
        CREATE TABLE people (
            ID PRIMARY KEY,
            USER VARCHAR(255) NOT NULL,
            TIMEDATE VARCHAR(255) NOT NULL,
            TWEETTEXT VARCHAR(255) NOT NULL,
            FLAG VARCHAR(50) NOT NULL,
            TARGET VARCHAR(20) NOT NULL
        )
        """
        cur.execute(create_command)
        conn.commit()
    except(Exception):
        print("Failed to create table")
    
    try:
        insert_command = """
        INSERT INTO people(ID, USER, TIMEDATE, TWEETTEXT, FLAG, TARGET)
        VALUES(%s, %s, %s, %s, %s, %s) RETURNING ID;
        """
        df=pd.read_csv('../data/data.csv')
        for i,r in df.iterrows():
            id = cur.execute(insert_command, (r['id'], r['user'], r['date'], r['text'], r['flag'], r['target']))
            print("Inserted person id " + id)
        print("Database insert complete")
    except(Exception):
        print("Failed to insert data")
    conn.commit()
    cur.close()
    conn.close()

# Cleaning Text Steps
# 1. Convert the letter into lowercase ('Squadron' is not equal to 'squadron').
# 2. Remove Twitter handles and hashtags.
# 3. Remove URL links, reference chars, and non-letter characters.
# 4. Remove punctuations like .,!? etc.
# 5. Perform Stemming upon the text.
def clean_text(text):
    # Normalize by converting text to lowercase.
    text = text.lower()
    
    # Remove Twitter handles and hashtags.
    text = " ".join([word for word in text.split() if word[0] != '@' and word[0] != '#'])
    
    # Remove URL links (http or https).
    text = re.sub(r'https?:\/\/\S+', '', text)
    # Remove URL links (with or without www).S
    text = re.sub(r"www\.[a-z]?\.?(com)+|[a-z]+\.(com)", '', text)
    
    # Remove HTML reference characters.
    text = re.sub(r'&[a-z]+;', '', text)
    # Remove non-letter characters.
    text = re.sub(r"[^a-z\s\(\-:\)\\\/\];='#]", '', text)
    
    # Remove all punctuations.
    punctuation_lst = list(string.punctuation)
    text = " ".join([word for word in text.split() if word not in (punctuation_lst)])
    # Remove stopwords.
    stop = stopwords.words('english')
    text = " ".join([word for word in text.split() if word not in (stop)])
    
    # Perform Stemming to remove prefixing within text.
    port = PorterStemmer()
    text = " ".join([port.stem(word) for word in text.split()])
    
    return text

default_args = {
    'owner': 'Ceffeinated Quantum Squadron',
    'start_date': dt.datetime(2023, 3, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    'banana',
    default_args=default_args,
    schedule='@once'
) as dag:
    print_starting = BashOperator(
        task_id="starting",
        bash_command='echo "Reading data"'
    )
    DBtask = PythonOperator(
        task_id='fill_database',
        python_callable=fill_database
    )
    print_starting >> DBtask