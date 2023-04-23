import re
import string
from collections import defaultdict

import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

#import tensorflow as tf
#from tensorflow.keras.models import load_model

class Preprocess():
    def __init__(self):
        pass
    
    # Construct a method that'll stem a word (remove prefixing).
    def stem_word(self, word):
        vowels = "aeiouy"
        prefixes = [("un", "re", "in", "dis"), ("en", "em", "ex")]
        suffixes = [("ing", ""), ("ed", ""), ("es", "s"), ("ly", ""), ("able", ""), ("ible", ""), ("ment", ""), ("ness", ""), ("ful", ""), ("ness", ""), ("ish", ""), ("ize", ""), ("ise", "")]

        # Handle special cases.
        if word in ["was", "is", "has", "does", "goes", "makes", "takes", "likes", "comes", "goes", "gets", "puts", "says", "knows", "sees", "needs", "uses"]:
            return word
        
        # Apply common rules.
        word = word.lower()
        word = re.sub(r"([a-z])([A-Z])", r"\1 \2", word)
        word = re.sub(r"[^a-zA-Z0-9\s]", "", word)
        
        # Remove prefixes.
        for prefix_set in prefixes:
            for prefix in prefix_set:
                if word.startswith(prefix):
                    word = word[len(prefix):]
                    break
        
        # Remove suffixes.
        for suffix_set in suffixes:
            for suffix in suffix_set:
                if word.endswith(suffix):
                    word = word[:len(word)-len(suffix)]
                    break
        
        # Remove duplicate consonants.
        new_word = ""
        for i in range(len(word)):
            if i > 0 and word[i] == word[i-1] and word[i] not in vowels:
                continue
            new_word += word[i]
        
        # Reduce vowels.
        for suffix in suffixes:
            for s in suffix:
                if new_word.endswith(s):
                    new_word = self.reduce_vowels(new_word)
                    break
        
        # Handle irregular words.
        if new_word in ["go", "have", "be", "do", "say", "make", "take", "see", "get", "come", "know", "put", "find", "give", "think", "tell", "feel", "become", "leave", "bring", "begin", "keep", "start", "hear", "stand", "lose", "win", "write", "sing"]:
            return new_word
        
        return(new_word)
    
    # Create a function to reduce vowels within a word.
    def reduce_vowels(self, word):
        new_word = ""
        vowels = "aeiouy"
        count = 0
        for i in range(len(word)):
            if i > 0 and word[i] in vowels and word[i] == word[i-1]:
                count += 1
            else:
                count = 0
            if count < 2:
                new_word += word[i]
        return(new_word)
    
    # Construct a task that cleans the text from tweets.
    def clean_tweet_data(self, tweet_data):    
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
                
                # Perform Stemming to remove prefixing within text.
                text = " ".join([self.stem_word(word) for word in text.split()])
                
                # Update the 'text' info with the cleaned version.
                tweet['text'] = text
                
            print("Data successfully cleaned")
                
        except(Exception) as error:
            print("Data failed to be cleaned:", error)
            
        return(tweet_data)
    
    # Construct a method to return the sentiment result of text.
    def obtain_tweet_sentiment(self, text):
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
    def classify_tweet_data(self, tweet_data):
        # Use ti.xcom_pull() to pull the returned value of extract_tweet_data task from XCom.
        #tweet_data = context['task_instance'].xcom_pull(task_ids='clean_tweet_data')

        try:
            for tweet in tweet_data:
                # Obtain the tweet text.
                text = tweet['text']

                # Obtain the sentiment of the tweet.
                sentiment = self.obtain_tweet_sentiment(text)

                # Update the 'sentiment' info with the sentiment result.
                tweet['sentiment'] = sentiment

            print("Tweets were successfully classified.")

        except(Exception) as error:
            print("Tweets failed to be classified.", error)

        return(tweet_data)