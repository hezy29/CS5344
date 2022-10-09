from preprocessing import *
import os
from pyspark import SparkConf, SparkContext

filedir = "datafiles"
filename = "16.txt"
processdir = "processed"
sparkdir = "spark"

processed_data = "".join(preprocessing(filedir + "/" + filename))[9:]
if not os.path.exists(processdir):
    os.mkdir(processdir)
f = open(os.path.abspath(processdir + "/" + filename), "w")
f.writelines(processed_data)
f.close()

if not os.path.exists(sparkdir):
    os.mkdir(sparkdir)
conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(processdir + "/" + filename)
words = lines.flatMap(lambda l: re.split(r"[^\w]+", l))
pairs = words.map(lambda w: (w, 1))
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2).repartition(1)
counts.saveAsTextFile(sparkdir + "/" + filename.split(".")[0])
sc.stop()
