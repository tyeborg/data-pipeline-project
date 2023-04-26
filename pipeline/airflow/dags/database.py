import csv
import json
import psycopg2
from airflow.models import Variable

# Define the filename for the CSV file.
FILENAME = './dags/data.csv'

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
            print("[+] Connection to PostgreSQL database successful")
        except(Exception, psycopg2.Error) as error:
            print("[-] Error while connecting to PostgreSQL database", error)
            
        return(conn)
    
    # Construct a method to close the cursor and connection.
    def close_connection(self, cur, conn):
        cur.close()
        conn.close()
        print("[+] Connection to PostgreSQL database successfully closed")
       
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
            drop_command = 'DROP TABLE IF EXISTS star_wars_comments'
            
            # Create the table to store our data in.
            create_command = '''
                CREATE TABLE IF NOT EXISTS star_wars_comments(
                    comment_id VARCHAR(200),
                    video_title VARCHAR(200),
                    author VARCHAR(200),
                    comment TEXT,
                    creation_date VARCHAR(200),
                    sentiment VARCHAR(200)
                )
            '''
            # Execute the following SQL queries.
            cur.execute(drop_command)
            cur.execute(create_command)
            
            # Commit the changes.
            conn.commit()
            print("[+] Table successfully created")
            
            # Close the cursor and connection.
            self.close_connection(cur, conn)
                
        except(Exception) as error:
            print("[-] Table failed to create:", error)
            
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
                INSERT INTO star_wars_comments(comment_id, video_title, author, comment, creation_date, sentiment)
                VALUES(%s, %s, %s, %s, %s, %s)
            '''
            # Loop through the data and insert it into the database.
            for row in tweet_data:
                cur.execute(insert_command, (row['comment_id'], row['video_title'], row['author'], row['comment'], row['date'], row['sentiment']))
               
            # Commit the changes.
            conn.commit()
            print("[+] Data successfully inserted into table")
            
            # Close the cursor and connection.
            self.close_connection(cur, conn)

        except(Exception) as error:
            print("[-] Data insertion failed:", error)
            # Close the cursor and connection.
            self.close_connection(cur, conn)
            
    # Define the function to retrieve data from PostgreSQL
    def get_data_from_database(self):
        # Initialize variable.
        cur = None
        data = None
            
        try:
            # Connect to the database.
            conn = self.create_connection()
            
            # Open a cursor to perform database operations.
            cur = conn.cursor()
            
            # Select ALL from the table.
            cur.execute('SELECT * FROM star_wars_comments')
            # Fetch all the rows as a list of tuples.
            data = cur.fetchall()
            
            print("[+] Data successfully fetched")
            
            # Close the cursor and connection.
            self.close_connection(cur, conn)
        
        except(Exception) as error:
            print("[-] Data fetch failed:", error)
            # Close the cursor and connection.
            self.close_connection(cur, conn)
            
        return(data)
    
    def save_into_csv(self, filename):
        # Obtain the rows as a list of tuples.
        rows = self.get_data_from_database()
        
        try:
            # Write the data to a CSV file.
            with open(filename, mode='w', newline='') as file:
                # Create a CSV writer object. 
                writer = csv.writer(file)
                
                # Initialize the header rows.
                writer.writerow(['comment_id', 'video_title', 'author', 'comment', 'date', 'sentiment'])
                # Insert the rows into the CSV.
                for row in rows:
                    writer.writerow(row)
            
            # Notify the user that this task was accomplished.        
            print("[+] Successfully copied data from PostgreSQL to CSV.")
            
        except(Exception) as error:
            print("[-] Failed to copy data to csv file:", error)