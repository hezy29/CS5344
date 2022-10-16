# Lab 2

> 2nd lab for CS5344 22 FALL

## Brief Introduction

Input documents directory `datafiles`, stopwords file `stopwords.txt`, and query file `query.txt`, and get an output of `PySpark` generated textfile directory `relevant_documents`, which includes 10 most relevant documents with respect to queries from `query.txt` and each document's most relevant sentence. The relevance of documents/sentences to query is defined by the cosine similarity between the **TF-IDF** value of each word and the one-hot vector of queries. 

## Data

- 80 documents of news, with a format of `excerpt`, `text`, and `comment`, stored in `datafiles` directory

- A list of stopwords to remove common words from the documents, stored in `stopwords.txt`

- A string of queries to search in documents as keywords, stored in `query.txt`

## Environment

> All experiments are conducted under `MacOS 11.6.8`

- `Python 3.10.8`
- `Scala 3.2.0`
- `PySpark 3.3.0`
  
  ## Command
  
  ```shell
  pip3 install --upgrade pyspark
  python3 main.py [PATH: datafiles] [PATH: stopwords] [PATH: query]
  ```
  
  the command should be fulfilled with prerequisites 
  
  ## Usage
- Run the command and set the correct path of the inputs


