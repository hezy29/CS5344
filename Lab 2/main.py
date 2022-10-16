import pyspark.context
from pyspark import SparkConf, SparkContext
import nltk
import re
import math
import os
import sys
import datetime


# Read Stopwords
def readstopwords(path="stopwords.txt"):
    with open(path) as f:
        stopwords = [word.rstrip("\n") for word in f.readlines()]
    f.close()
    return stopwords


# Read Query
def readquery(path="query.txt"):
    with open(path) as f:
        query = f.readline().split()
    f.close()
    return query


# Path
start = datetime.datetime.now()
if len(sys.argv) > 1:
    dir_path, stopwords_path, query_path = sys.argv[1], sys.argv[2], sys.argv[
        3]
    if not dir_path[-1] == "/":
        dir_path += "/"
    stopwords = readstopwords(stopwords_path)
    query = readquery(query_path)
else:
    dir_path = "datafiles/"
    stopwords = readstopwords()
    query = readquery()

conf = SparkConf()
sc = SparkContext(conf=conf)


def read_file(sc: pyspark.context.SparkContext, dirname: str, filename: str,
              stopwords: list):
    pattern = re.compile(r"\w+.")
    if not dirname[-1] == "/":
        dirname += "/"
    file_wordcount = sc.textFile(dirname + filename). \
        flatMap(lambda x: nltk.word_tokenize(re.sub(r"'", "", x.lower()))). \
        filter(pattern.match). \
        filter(lambda x: x not in stopwords). \
        map(lambda x: (x, 1)). \
        reduceByKey(lambda n1, n2: n1 + n2). \
        map(lambda x: (filename, x))
    return file_wordcount


def read_file_sentence(sc: pyspark.context.SparkContext, dirname: str,
                       filename: str):
    if not dirname[-1] == "/":
        dirname += "/"
    doc = sc.textFile(dirname + filename). \
        map(lambda word: nltk.word_tokenize(re.sub(r"'", "", word.lower())))
    # filter(pattern.match). \
    # filter(lambda x: x not in stopwords)
    return doc


def computefreq(sc: pyspark.context.SparkContext, sentence: list, index: int):
    freq = sc.parallelize(sentence). \
        map(lambda x: (x, 1)). \
        reduceByKey(lambda n1, n2: n1 + n2). \
        map(lambda x: (index, x))
    return freq


def computeTF(words: pyspark.rdd.RDD):

    def f(x):
        return {y[0]: y[1] for y in x}

    return words.groupByKey().mapValues(f).collect()[0]


def computeIDF(sc: pyspark.context.SparkContext, word_list: list, N: int):
    DF = sc.parallelize(word_list). \
        flatMap(lambda x: list(x[1].keys())). \
        map(lambda word: (word, 1)). \
        reduceByKey(lambda n1, n2: n1 + n2)
    IDF = DF.map(lambda x: (x[0], math.log10(N / x[1])))
    return IDF


def computeTFIDF(tf, idf, normalize=True):
    tfidf = {}
    for word in idf.keys():
        if word in tf.keys():
            tfidf[word] = (1 + math.log10(tf[word])) * idf[word]
        else:
            tfidf[word] = 0
    if normalize:
        tfidf_norm = sum([x**2 for x in tfidf.values()])
        if tfidf_norm == 0:
            return tfidf
        tfidf = {k: v / tfidf_norm for k, v in tfidf.items()}
    return tfidf


def computeRelevance(tfidf, query):
    if len(tfidf) == 0 or len(query) == 0:
        return 0
    else:
        return sum([tfidf[q] if q in tfidf.keys() else 0 for q in query]) / \
            math.sqrt(len(query)) / \
            math.sqrt(sum(x ** 2 for x in tfidf.values()))


# documents
N = len(os.listdir(dir_path))

tf_list = []
for file in os.listdir(dir_path):
    words = read_file(sc, dir_path, file, stopwords)
    tf_list.append(computeTF(words))

tf_rdd = sc.parallelize(tf_list)
idf_dict = computeIDF(sc, tf_list, N).collectAsMap()
tfidf_rdd = tf_rdd.map(lambda x: (x[0], computeTFIDF(x[1], idf_dict)))
relevance_rdd = tfidf_rdd.map(lambda x: (x[0], computeRelevance(x[1], query)))

print(relevance_rdd.top(10, key=lambda x: x[1]))

relevant_sentences = []
for x in relevance_rdd.top(10, key=lambda x: x[1]):
    file = x[0]
    doc = read_file_sentence(sc, dir_path, file)

    n = doc.count()

    sentence_tf_list = []
    pattern = re.compile(r'\w.+')

    for s, idx in zip(doc.collect(), range(N)):
        s = sc.parallelize(s).filter(pattern.match).filter(
            lambda word: word not in stopwords).collect()
        freq = computefreq(sc, s, idx)
        sentence_tf_list.append(computeTF(freq))

    sentence_tf_rdd = sc.parallelize(sentence_tf_list)
    sentence_idf_dict = computeIDF(sc, sentence_tf_list, n).collectAsMap()
    sentence_tfidf_rdd = sentence_tf_rdd.map(
        lambda x: (x[0], computeTFIDF(x[1], sentence_idf_dict)))
    sentence_relevance_rdd = sentence_tfidf_rdd.map(
        lambda x: (x[0], computeRelevance(x[1], query)))
    relevant_sentence = sentence_relevance_rdd.top(1, key=lambda x: x[1])

    relevant_sentences.append(
        (sc.textFile(dir_path + file).collect()[relevant_sentence[0][0]],
         relevant_sentence[0][1]))

relevant_docs = [(x, y) for x, y in zip(
    relevance_rdd.top(10, key=lambda x: x[1]), relevant_sentences)]

output = sc.parallelize(relevant_docs).map(
    lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])).repartition(1)
output.saveAsTextFile(path="relevant_documents")

sc.stop()

end = datetime.datetime.now()
print("Time: {} minutes".format((end - start).seconds / 60))
