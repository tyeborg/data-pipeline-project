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

nltk.download('vader_lexicon')
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import psycopg2
import datetime as dt
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable, Connection
from airflow.operators.python_operator import PythonOperator

# Construct a method to extract tweet data.
def extract_tweet_data():
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
            
        print("Data Extraction: Success")
        
    except(Exception):
        print("Data Extraction: Fail")
        
    return tweet_data

# Construct a method that cleans the text from tweets.
def clean_tweet_data(**context):
    
    # Use ti.xcom_pull() to pull the returned value of extract_tweet_data task from XCom.
    tweet_data = context['task_instance'].xcom_pull(task_ids='extract_tweet_data')
    
    try:
        # Iterate through each tweet info in tweet_data.
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
            
            print("\nOriginal: {}".format(tweet['text']))
            
            # Update the 'text' info with the cleaned version.
            tweet['text'] = text
            
            print("Cleaned: {}".format(tweet['text']))
            
        print("Data Cleaning: Success")
            
    except(Exception):
        print("Data Cleaning: Failed")
        
    return tweet_data

# Construct a method to return the sentiment result of text.
def obtain_tweet_sentiment(tweet_texts):
    # Initialize the sentiment analyzer.
    analyzer = SentimentIntensityAnalyzer()

    # Initialize list to store sentiments.
    sentiments = []

    for tweet_text in tweet_texts:
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

        # Add the sentiment to the list.
        sentiments.append(sentiment)

    # Return the list of sentiments.
    return sentiments


# Define the function to classify the sentiment of the tweet text.
def classify_tweets(**context):
    # Use ti.xcom_pull() to pull the returned value of extract_tweet_data task from XCom.
    tweet_data = context['task_instance'].xcom_pull(task_ids='clean_tweet_data')

    try:
        for tweet in tweet_data:
            # Obtain the tweet text.
            text = tweet['text']

            # Obtain the sentiment of the tweet.
            sentiment = obtain_tweet_sentiment(text)

            # Update the 'sentiment' info with the sentiment result.
            tweet['sentiment'] = sentiment

        print("Tweet Classification: Success")

    except(Exception):
        print("Tweet Classification: Fail")

    return tweet_data

# Define the function to send data to Postgres.    
def store_data(**context):
    # Get the connection details from Airflow variable.
    connection_id = 'postgres_connection'
    connection = Variable.get(connection_id, deserialize_json=True)
    
    # From Airflow variable, intialize varaibles.
    db_host = connection['host']
    db_port = connection['port']
    db_name = connection['schema']
    db_user = connection['login']
    db_password = connection['password']
    
    # Use ti.xcom_pull() to pull the returned value of extract_tweet_data task from XCom.
    tweet_data = context['task_instance'].xcom_pull(task_ids='clean_tweet_data')
    
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
        # Ensure to the user that a connection has been made.
        print("Database Connection: Success")
        
        # Open a cursor to perform database operations.
        cur = conn.cursor()
        
         # Create the table to store our data in.
        create_command = """
            CREATE TABLE TWEETS (
                ID SERIAL PRIMARY KEY,
                USER VARCH(25) NOT NULL,
                TIMEDATE VARCHAR(75) NOT NULL,
                TWEETTEXT VARCHAR(255) NOT NULL,
                TARGET VARCHAR(25)
            );
        """
        
        # Execute the following SQL query to construct the table.
        cur.execute(create_command)
        
        # Commit the changes.
        conn.commit()
        print("Table Creation: Success")
            
    except(Exception):
        print("Table Creation: Fail")
        
    try:
        # Define the SQL statement to insert data.
        insert_command = """
            INSERT INTO TWEETS(ID, USER, TIMEDATE, TWEETTEXT, TARGET)
            VALUES(%s, %s, %s, %s, %s);
        """
        
        # Loop through the data and insert it into the database.
        for row in tweet_data:
            cur.execute(insert_command, (row['id'], row['user'], row['created_at'], row['text'], row['sentiment']))

        # Commit the changes.
        conn.commit()
        print("Data Insertion: Success")

    except(Exception):
        print("Data Insertion: Fail")
    
    # Close the cursor and connection
    cur.close()
    conn.close()

default_args = {
    'owner': 'Caffeinated Quantum Squadron',
    'start_date': dt.datetime(2023, 3, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}
# Define the DAG.
dag = DAG(
    dag_id = 'boop42',
    description='A pipeline for analyzing Twitter sentiment',
    default_args=default_args,
    schedule='@once'
)
extract_task = PythonOperator(
    task_id='extract_tweet_data',
    python_callable=extract_tweet_data,
    provide_context=True,
    dag=dag
)
clean_task = PythonOperator(
    task_id='clean_tweet_data',
    python_callable=clean_tweet_data,
    provide_context=True,
    dag=dag
)
classify_task = PythonOperator(
    task_id='classify_tweets',
    python_callable=classify_tweets,
    provide_context=True,
    dag=dag
)
store_task = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    provide_context=True,
    dag=dag
)

extract_task >> clean_task >> classify_task >> store_task

if __name__ == "__main__":
    dag.cli()
    