import re
import string
from collections import defaultdict

import nltk
nltk.download('words')
from nltk.corpus import words
nltk.download('vader_lexicon')
from nltk.stem import PorterStemmer
from nltk.sentiment.vader import SentimentIntensityAnalyzer

class Preprocess():
    def __init__(self):
        pass
    
    def filter_comment(self, comment):
        # Normalize by converting text to lowercase.
        comment = comment.lower()
        
        # Remove hashtags.
        comment = " ".join([word for word in comment.split() if word[0] != '#'])
        
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
        # Initialize a set of English vocabulary words to verfiy
        # the comment to be in the English language.
        english_vocab = set(words.words()) 
        
        # Tokenize the comment.
        words = nltk.wordpunct_tokenize(comment)
        
        # Determine if each word belongs to the english dictionary.
        english_words = [word for word in words if word in english_vocab]
        if len(english_words) / len(words) >= 0.5:
            return True
        else:
            return False
        
    def discard_unwanted_comments(self, comment_data, unwanted_lst):
        # Create a duplicate of the comment_data to avoid comment removal errors.
        duplicate_data = comment_data 
        
        for comment in duplicate_data:
            # Declare the comment from the dictionary.
            comment_text = comment['comment']
            
            if comment_text in unwanted_lst:
                # Remove comment from comment_data.
                comment_data.remove(comment)
                
        return(comment_data)
            
    # Construct a task that cleans the text from comments.
    def clean_comment_data(self, comment_data):  
        try:
            # Initialize a list to get rid of the unwanted comments.
            unwanted = [] 
            
            # Iterate through each comment info in comment_data.
            for comment in comment_data:
                # Declare the comment from the dictionary.
                comment_text = comment['comment']
                
                # ULTRA clean the comment.
                comment_text = self.filter_comment(comment_text)
                
                # Update the 'comment' info with the cleaned version.
                comment['comment'] = comment_text
                
                if self.is_english(comment_text) == True:
                    # Perform Stemming to remove prefixing within text.
                    # Create a stemmer object
                    stemmer = PorterStemmer()
                    comment_text = " ".join([stemmer.stem(word) for word in comment_text.split()])
                
                    # Update the 'comment' info with the stemmed version.
                    comment['comment'] = comment_text
                else:
                    # If comment is not in the English language, discard it.
                    unwanted.append(comment['comment'])
                    
            # Discard the unwanted comments.
            comment_data = self.discard_unwanted_comments(comment_data, unwanted)
            
            # Convert comment_data list to a set to remove duplicates.
            comment_data_set = set(tuple(comment.items()) for comment in comment_data)
            
            # Convert back to a list.
            comment_data = [dict(comment) for comment in comment_data_set]
                
            print("Data successfully cleaned")
                
        except(Exception) as error:
            print("Data failed to be cleaned:", error)
            
        return(comment_data)
    
    # Construct a method to return the sentiment result of text.
    def obtain_comment_sentiment(self, text):
        # Initialize the sentiment analyzer.
        analyzer = SentimentIntensityAnalyzer()
        
        # Initialize the sentiment variable.
        sentiment = ''

        # Obtain the sentiment score.
        score = analyzer.polarity_scores(text)

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

            print("Tweets were successfully classified.")

        except(Exception) as error:
            print("Tweets failed to be classified.", error)

        return(comment_data)