import re
import nltk


def preprocessing(filepath, stopwords):
    with open(filepath) as f:
        data = [re.sub(r"[^\w\s]", "", x.lower()) for x in f.readlines()][1:-1]
    f.close()
    tokens = nltk.word_tokenize("".join(data)[9:])
    return " ".join([word for word in tokens if not word in stopwords])
