import os
from pyspark import SparkConf, SparkContext
import shutil
import numpy as np
import nltk
import tfidf
import re

# Directory
filedir = "datafiles"

# Stopword list
stopwords_path = "stopwords.txt"
with open("stopwords.txt") as f:
    stopwords = [re.sub(r"[^\w\s]", "", x[:-1]) for x in f.readlines()]
f.close()


# Query list
with open("query.txt") as f:
    query = f.readline().split()
f.close()


# Document list
documents_list, tokens_list = [], []
if not os.path.exists("processed"):
    os.mkdir("processed")

for datafile in [str(x) + ".txt" for x in [76, 4448, 1710]]:
    with open(filedir + "/" + datafile) as f:
        doc = [x.lower() for x in f.readlines()][1:-1]
    f.close()
    tokens = nltk.word_tokenize(
        "".join(re.compile("[\u4E00-\u9FA5|\s\w]").findall(" ".join(doc)[9:]))
    )
    # sentences = "\n".join(doc)[9:].split("\n")
    tokens_list.append(
        {
            "filename": datafile,
            "data": [word for word in tokens if not word in stopwords],
        }
    )
    # documents_list.append(
    #     {
    #         "filename": datafile,
    #         "data": sentences[::2],
    #     }
    # )

    f = open(os.path.abspath("processed/" + datafile), "w")
    f.writelines(" ".join([word for word in tokens if not word in stopwords]))
    f.close()


# tokens_list = [
#     {"filename": 1, "data": ["like", "banana", "cake"]},
#     {"filename": 2, "data": ["like", "banana", "banana", "milk"]},
#     {"filename": 3, "data": ["good", "night"]},
# ]
# query = ["banana", "milk", "night"]

tokens_list2 = []
for datafile in os.listdir("processed"):
    with open("processed/" + datafile) as f:
        tokens_list2.append(
            {
                "filename": datafile,
                "data": [x for x in f.readline().split() if not x in stopwords],
            }
        )

    f.close()


conf = SparkConf()
sc = SparkContext(conf=conf)

doc_tfidf, words_idf_dic = tfidf.tfidf(tokens_list, sc)
doc_tfidf2, words_idf_dic2 = tfidf.tfidf(tokens_list2, sc)


# query one-hot vector
"""????????????????????????????????????????"""
query_onehot = [0 if not x in query else 1 for x in words_idf_dic.keys()]
query_onehot2 = [0 if not x in query else 1 for x in words_idf_dic2.keys()]

# relevance


def computeRelevance(normalized_tfidf_vector, query_onehot_vector):
    a, b = np.array(normalized_tfidf_vector), np.array(query_onehot_vector)
    return a.dot(b) / (np.linalg.norm(a) * np.linalg.norm(b))


# cosine relevance
def f(doc_tfidf, query_onehot):
    document_rdd = sc.parallelize(doc_tfidf)
    normalized_document_tfidf_rdd = document_rdd.map(lambda x: x[0])
    document_id_rdd = document_rdd.map(lambda x: x[1])
    relevance_document_rdd = normalized_document_tfidf_rdd.map(
        lambda tfidf_vector: computeRelevance(tfidf_vector, query_onehot)
    ).zip(document_id_rdd)
    relevance_vectors = relevance_document_rdd.collect()

    # top 10 relevant documents
    print(relevance_document_rdd.top(10, key=lambda x: x[0]))
    top_10_index = [
        relevance_vectors.index(x)
        for x in relevance_document_rdd.top(1, key=lambda x: x[0])
    ]
    top_10_datafiles = [os.listdir("datafiles")[x] for x in top_10_index]
# top_10_docs = [documents_list[x] for x in top_10_index]


f(doc_tfidf, query_onehot)
f(doc_tfidf2, query_onehot2)

# print(top_10_datafiles)

# for doc in top_10_docs:
#     tokens_docs = [x.split() for x in doc]
#     sentence_tfidf, words_idf_dic = tfidf.tfidf(tokens_docs, sc)

#     # query one-hot vector
#     query_onehot = [0 if not x in query.split() else 1 for x in words_idf_dic.keys()]

#     # cosine relevance
#     normalized_sentence_tfidf_rdd = sc.parallelize(sentence_tfidf)
#     relevance_sentence_rdd = normalized_sentence_tfidf_rdd.map(
#         lambda tfidf_vector: computeRelevance(tfidf_vector, query_onehot)
#     )
#     relevance_vectors = relevance_sentence_rdd.collect()
#     # print(relevance_vectors)

#     most_relevant_index = relevance_vectors.index(relevance_sentence_rdd.max())


sc.stop()
