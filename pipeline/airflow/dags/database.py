import json
import psycopg2
from airflow.models import Variable

class Database():
    def __init__(self):
        pass
    
    # Create a function that'll establish a connection to the database.
    def create_connection(self):
        # Get the connection details from Airflow variable.
        connection_id = 'postgres_connection'
        connection = Variable.get(connection_id, deserialize_json=True)
        
        # From Airflow variable, intialize varaibles.
        db_host = connection['host']
        db_port = connection['port']
        db_name = connection['schema']
        db_user = connection['login']
        db_password = connection['password']
        
        conn = None
        try:
            # Connect to the database.
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
            print("Connection to PostgreSQL database successful")
        except(Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL database", error)
            
        return(conn)
    
    # Construct a method to close the cursor and connection.
    def close_connection(self, cur, conn):
        cur.close()
        conn.close()
        print("Connection to PostgreSQL database successfully closed")
       
    # Create a function that'll create a table within the database.
    def create_table(self):
        # Initialize variable.
        cur = None
        
        try:
            # Connect to the database.
            conn = self.create_connection()
            
            # Open a cursor to perform database operations.
            cur = conn.cursor()
            
            # Initilaize drop table command only if it exists.
            drop_command = 'DROP TABLE IF EXISTS tweets'
            
            # Create the table to store our data in.
            create_command = '''
                CREATE TABLE IF NOT EXISTS tweets(
                    id VARCHAR(200),
                    username VARCHAR(200),
                    created_at VARCHAR(200),
                    text VARCHAR(200),
                    sentiment VARCHAR(200),
                    CONSTRAINT primary_key_constraint PRIMARY KEY (id)
                )
            '''
            # Execute the following SQL queries.
            cur.execute(drop_command)
            cur.execute(create_command)
            
            # Commit the changes.
            conn.commit()
            print("Table successfully created")
            
            # Close the cursor and connection.
            self.close_connection(cur, conn)
                
        except(Exception) as error:
            print("Table failed to create:", error)
            
    # Define the task to send data to Postgres.
    def store_data(self, tweet_data):    
        # Initialize variable.
        cur = None
            
        try:
            # Connect to the database.
            conn = self.create_connection()
            
            # Open a cursor to perform database operations.
            cur = conn.cursor()
            
            # Define the SQL statement to insert data.
            insert_command = '''
                INSERT INTO tweets(id, username, created_at, text, sentiment)
                VALUES(%s, %s, %s, %s, %s)
            '''
            # Loop through the data and insert it into the database.
            for row in tweet_data:
                cur.execute(insert_command, (row['id'], row['user'], row['created_at'], row['text'], row['sentiment']))

            # Commit the changes.
            conn.commit()
            print("Data successfully inserted")
            
            # Close the cursor and connection.
            self.close_connection(cur, conn)

        except(Exception) as error:
            print("Data failed to insert:", error)
            # Close the cursor and connection.
            self.close_connection(cur, conn)