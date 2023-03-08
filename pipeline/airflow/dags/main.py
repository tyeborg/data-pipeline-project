# Import the appropriate libraries for use.
import re
import json
#import nltk
import string
#import nltk.corpus
import pandas as pd
#nltk.download('stopwords')
#from nltk.corpus import stopwords
#from nltk.stem.porter import PorterStemmer
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer


import psycopg2
import datetime as dt
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# Construct a method to extract tweet data.
def extract_tweet_data():
    # Define the data to be extracted.
    df=pd.read_csv('../data/data.csv')
    
    # Initialize a list to store tweet data.
    tweet_data = []
    
    for idx, row in df.iterrows():
        tweet = {
            'id': row['id'],
            'created_at': row['date'],
            'text': row['text'],
            'user': row['user']
        }
        tweet_data.append(tweet)
        
    return tweet_data

# Cleaning Text Steps
# 1. Convert the letter into lowercase ('Squadron' is not equal to 'squadron').
# 2. Remove Twitter handles and hashtags.
# 3. Remove URL links, reference chars, and non-letter characters.
# 4. Remove punctuations like .,!? etc.
# 5. Perform Stemming upon the text.
def clean_tweet_data(tweet_data):
    # Args: tweet_data (dict): A dictionary containing tweet data, including the tweet text.
    # Returns: dict: A dictionary containing cleaned tweet data.
    
    for tweet in tweet_data:
        # Declare the tweet text from the dictionary.
        text = tweet['text']
        
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
        #stop = stopwords.words('english')
        #text = " ".join([word for word in text.split() if word not in (stop)])
        
        # Perform Stemming to remove prefixing within text.
        #port = PorterStemmer()
        #text = " ".join([port.stem(word) for word in text.split()])
        
        # Update the 'text' info with the cleaned version.
        tweet['text'] = text
    
    return tweet_data

# Construct a method to return the sentiment result of text.
def obtain_tweet_sentiment(tweet_text):
    
    # Initialize the sentiment analyzer.
    analyzer = SentimentIntensityAnalyzer()
    
    # Obtain the sentiment score.
    score = analyzer.polarity_scores(tweet_text)
    
    # Obtain the compound score.
    compound = score['compound']
    
    # Classify the tweet sentiment based on the compound score.
    # If the compound score is greater than 0.05, the tweet is classified as positive.
    if compound >= 0.05:
        sentiment = 'positive'
    # If the compound score is less than -0.05, the tweet is classified as negative.
    elif compound <= -0.05:
        sentiment = 'negative'
    # If the compound score is between -0.05 and 0.05, the tweet is classified as neutral.
    else:
        sentiment = 'neutral'
    
    # Return the sentiment.
    return sentiment


# METHOD IS STILL BEING REFINED...
# Define the function to send data to Postgres.    
def store_tweet_sentiment():
    # Get the connection details from Airflow variables.
    connection_id = 'postgres_connection'
    connection = Variable.get(connection_id, deserialize_json=True)
    
    # HOST = "postgres"
    db_host = connection['host']
    # PORT = "5432"
    db_port = connection['port']
    # DATABASE = "postgres"
    db_name = connection['schema']
    # USER = "airflow"
    db_user = connection['login']
    # PASSWORD = "airflow"
    db_password = connection['password']
    
    # Initialize variable.
    cur = None
    
    try:
        # Connect to the database.
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        # Open a cursor to perform database operations.
        cur = conn.cursor()
        
        # Create the table to store our data in.
        create_command = """
        CREATE TABLE tweet (
            id SERIAL PRIMARY KEY,
            user VARCHAR(255) NOT NULL,
            timedate VARCHAR(255) NOT NULL,
            tweettext VARCHAR(255) NOT NULL,
            flag VARCHAR(50) NOT NULL,
            target VARCHAR(20) NOT NULL
        )
        """
        # Execute the following SQL query to construct the table.
        cur.execute(create_command)
        
        # Commit the changes.
        conn.commit()
        print("Table has been created!")
    except(Exception):
        print("Failed to create table")
        
    try:
        # Define the SQL statement to insert data.
        insert_command = """
        INSERT INTO tweet(ID, USER, TIMEDATE, TWEETTEXT, FLAG, TARGET)
        VALUES(%s, %s, %s, %s, %s, %s);
        """
        # Define the data to be inserted.
        df=pd.read_csv('../data/data.csv')
        
        # Loop through the data and insert it into the database.
        for idx, row in df.iterrows():
            cur.execute(insert_command, (row['id'], row['user'], row['date'], row['text'], row['flag'], row['target']))
        print("Database insert complete")

        # Commit the changes.
        conn.commit()

    except(Exception):
        print("Failed to insert data")
    
    # Close the cursor and connection
    cur.close()
    conn.close()


# Define the function to classify the sentiment of the tweet text.
def classify_tweet_sentiment(tweet_data):
    # Args: tweet_data (dict): A dictionary containing tweet data, including the tweet text.
    # Returns: dict: A dictionary containing tweet data, including the tweet text and sentiment.
    
    for tweet in tweet_data:
        # Obtain the tweet text.
        text = tweet['text']
        
        # Obtain the sentiment of the tweet.
        sentiment = obtain_tweet_sentiment(text)
        
        # Update the 'sentiment' info with the sentiment result.
        tweet['sentiment'] = sentiment
    
    return tweet_data


default_args = {
    'owner': 'Caffeinated Quantum Squadron',
    'start_date': dt.datetime(2023, 3, 5),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}
# Define the DAG.
dag = DAG(
    dag_id = 'awesome_sauces',
    description='A pipeline for analyzing Twitter sentiment',
    default_args=default_args,
    schedule='@once'
)
extract_tweet_task = PythonOperator(
    task_id='extract_tweet_data',
    python_callable=extract_tweet_data,
    dag=dag
)
clean_task = PythonOperator(
    task_id='clean_tweet_data',
    python_callable=clean_tweet_data,
    op_kwargs={'tweet_data': '{{ ti.xcom_pull(task_ids="extract_tweet_data") }}'},
    dag=dag
)
# Define the operator to run the store function.
#store_task = PythonOperator(
    #task_id='store_into_database',
    #python_callable=store_tweet_sentiment,
    #dag=dag
#)

extract_tweet_task >> clean_task

if __name__ == "__main__":
    #dag.cli()
    data = extract_tweet_data()
    print(data[56]['text'])
    
    cleaned_data = clean_tweet_data(data)
    print(cleaned_data[56]['text'])
    