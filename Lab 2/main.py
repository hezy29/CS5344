from preprocessing import *
import os
from pyspark import SparkConf, SparkContext
import shutil

filedir = "datafiles"
processdir = "processed"
sparkdir = "spark"

# Stopword list
stopwords_path = "stopwords.txt"
with open("stopwords.txt") as f:
    stopwords = [re.sub(r"[^\w\s]", "", x)[:-1] for x in f.readlines()]
f.close()

if not os.path.exists(processdir):
    os.mkdir(processdir)

if not os.path.exists(sparkdir):
    os.mkdir(sparkdir)

conf = SparkConf()
sc = SparkContext(conf=conf)

for datafile in os.listdir(filedir):
    processed_data = preprocessing(
        filepath=filedir + "/" + datafile, stopwords=stopwords
    )
    f = open(os.path.abspath(processdir + "/" + datafile), "w")
    f.writelines(processed_data)
    f.close()

    lines = sc.textFile(processdir + "/" + datafile)
    words = lines.flatMap(lambda l: re.split(r"[^\w]+", l))
    pairs = words.map(lambda w: (w, 1))
    counts = pairs.reduceByKey(lambda n1, n2: n1 + n2).repartition(1)
    path = sparkdir + "/" + datafile.split(".")[0]
    if os.path.exists(path):
        shutil.rmtree(path)
    counts.saveAsTextFile(path)

sc.stop()
