import re


def preprocessing(filepath):
    with open(filepath) as f:
        data = [re.sub(r"[^\w\s]", "", x.lower()) for x in f.readlines()][1:-1]
    f.close()
    return data
