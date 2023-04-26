import re
import string
from datetime import datetime
from collections import defaultdict

import nltk
nltk.download('words')
nltk.download('wordnet')
nltk.download('vader_lexicon')
from nltk.stem import WordNetLemmatizer
from nltk.corpus import words
from nltk.sentiment.vader import SentimentIntensityAnalyzer

class Preprocess():
    def __init__(self):
        pass
    
    def filter_comment(self, comment):
        # Normalize by converting text to lowercase.
        comment = comment.lower()
        
        # Remove hashtags and email addresses.
        comment = " ".join([word for word in comment.split() if word[0] != '#'])
        comment = re.sub(r'[\w\.-]+@[\w\.-]+', '', comment)
        
        # Remove URL links (http or https).
        comment = re.sub(r'https?:\/\/\S+', '', comment)
        # Remove URL links (with or without www).S
        comment = re.sub(r"www\.[a-z]?\.?(com)+|[a-z]+\.(com)", '', comment)
        
        # Remove HTML reference characters.
        comment = re.sub(r'&[a-z]+;', '', comment)
        # Remove non-letter characters.
        comment = re.sub(r"[^a-z\s\(\-:\)\\\/\];='#]", '', comment)
        
        # Remove all punctuations.
        punctuation_lst = list(string.punctuation)
        comment = " ".join([word for word in comment.split() if word not in (punctuation_lst)])
        
        # Return cleaned comment.
        return(comment)
    
    def is_english(self, comment):
        # Initialize a list of english vocabulary words.
        english_vocab = set(w for w in words.words())
        
        # Tokenize the comment.
        word_tokens = nltk.wordpunct_tokenize(comment)
        
        # Initialize a list to store all the english words within comment.
        english_words = []
        
        # Determine if each word belongs to the english dictionary.
        for w in word_tokens:
            # Filter out short words and words with no letters.
            if len(w) < 2 or not any(c.isalpha() for c in w):
                continue
            
            if w in english_vocab:
                # Add the english word to the english_words list.
                english_words.append(w)
            else:
                continue
        
        # Figure out if the majority of the text was in English.
        if len(english_words) == 0:
            return False
        elif len(english_words) / len(word_tokens) >= 0.5:
            return True
        else:
            return False
        
    def discard_unwanted_comments(self, comment_data, unwanted_lst):
        # Create a shallow copy of the comment_data to avoid comment removal errors.
        duplicate_data = comment_data.copy()
        
        for comment in duplicate_data:
            # Declare the comment id from the dictionary.
            comment_id = comment['comment_id']
            
            if comment_id in unwanted_lst:
                # Remove comment info dictionary from comment_data.
                comment_data.remove(comment)
                
        return(comment_data)
    
    # Create a method that converts date string to YYYY-MM-DD format.
    def convert_date(self, date_str):
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        return date_obj.strftime("%Y-%m-%d")
            
    # Construct a method that cleans the text from comments.
    def clean_comment_data(self, comment_data):  
        try:
            # Initialize a list to get rid of the unwanted comments.
            unwanted = [] 
            
            # Iterate through each comment info in comment_data.
            for comment in comment_data:
                # Convert date to the YYYY-MM-DD format.
                comment['date'] = self.convert_date(comment['date'])
                
                # Declare the comment from the dictionary.
                comment_text = comment['comment']
                # ULTRA clean the comment.
                comment_text = self.filter_comment(comment_text)
                
                # Receive a character count of the comment text.
                comment_char_count = len(comment_text)
                
                # Determine if the comment is mostly comprised of english and exceeds 60 characters.
                if self.is_english(comment_text) == True and comment_char_count > 60:
                    # Perform lemmatization.
                    lemmatizer = WordNetLemmatizer()
                    comment_text = " ".join([lemmatizer.lemmatize(word) for word in comment_text.split()])
                
                    # Update the 'comment' info with the stemmed version.
                    comment['comment'] = comment_text
                else:
                    # If comment is not in the English language, discard it.
                    unwanted.append(comment['comment_id'])
                    
            # Discard the unwanted comments.
            comment_data = self.discard_unwanted_comments(comment_data, unwanted)
            
            # Convert comment_data list to a set to remove duplicates.
            comment_data_set = set(tuple(comment.items()) for comment in comment_data)
            
            # Convert back to a list.
            comment_data = [dict(comment) for comment in comment_data_set]
                
            print("[+] Data successfully cleaned")
                
        except(Exception) as error:
            print("[-] Data failed to be cleaned:", error)
            
        return(comment_data)
    
    # Construct a method to return the sentiment result of text.
    def obtain_comment_sentiment(self, text):
        # Initialize the sentiment analyzer and sentiment variable.
        analyzer = SentimentIntensityAnalyzer()
        sentiment = ''
        
        # Initialize threshold parameters.
        positive_threshold = 0.05
        negative_threshold = -0.05

        # Obtain the sentiment scores.
        sentiment_score = analyzer.polarity_scores(text)

        # Classify the sentiment based on the compound score.
        compound = sentiment_score['compound']
        switcher = {
            compound >= positive_threshold: 'positive',
            compound <= negative_threshold: 'negative',
        }
        sentiment = switcher.get(True, 'neutral')

        # Return the sentiment.
        return(sentiment)
    
    # Define the function to classify the sentiment of the tweet text.
    def classify_comment_data(self, comment_data):
        try:
            for row in comment_data:
                # Obtain the tweet text.
                text = row['comment']

                # Obtain the sentiment of the tweet.
                sentiment = self.obtain_comment_sentiment(text)

                # Update the 'sentiment' info with the sentiment result.
                row['sentiment'] = sentiment

            print("[+] Comments were successfully classified")

        except(Exception) as error:
            print("[-] Comments failed to be classified:", error)

        return(comment_data)
