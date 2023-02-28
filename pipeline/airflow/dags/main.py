# Import the appropriate libraries for use.
import re
import nltk
import string

import nltk.corpus
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

# Cleaning Text Steps
# 1. Convert the letter into lowercase ('Squadron' is not equal to 'squadron').
# 2. Remove Twitter handles and hashtags.
# 3. Remove URL links, reference chars, and non-letter characters.
# 4. Remove punctuations like .,!? etc.
# 5. Perform Stemming upon the text.

# Construct a method that will clean text in an efficient manner.
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