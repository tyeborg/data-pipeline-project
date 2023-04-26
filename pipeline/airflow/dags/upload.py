from datetime import datetime
import subprocess, json, kaggle, os
from kaggle.api.kaggle_api_extended import KaggleApi

class KaggleUpload():
    def __init__(self):  
        # Obtain Kaggle username.
        self.username = "tylersupersad"
        # Set the ID of the Kaggle dataset to upload the CSV file to.
        self.dataset_id = f'{self.username}/star-wars-youtube-comments-sentiment'
    
    def establish_conn(self):
        api = None
        try:
            # Create the KAggle API connection.
            api = KaggleApi()
            print("[+] Created the Kaggle API connection")
            return(api)
        except(Exception) as error:
            print("[-] Failed to establish connection to Kaggle API:", error)
        
    def upload(self):
        # Create the KAggle API connection.
        api = self.establish_conn()
        
        try:
            # Authenticate to the Kaggle API.
            api.authenticate()
            
            # Upload the CSV file to Kaggle and get the file ID
            folder_name = 'dags'
            
            # Initialize a present timestamp.
            timestamp = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            api.dataset_create_version(
                folder_name, f"Updated using airflow at {timestamp}"
            )
            print(f'[+] Pushed the new version to Kaggle at {timestamp}')
            
        except(Exception) as error:
            print("[-] Failed to push new version to Kaggle:", error)