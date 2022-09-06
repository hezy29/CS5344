import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.config(
    "spark.some.config.option", "some-value"
).getOrCreate()

meta_df = spark.read.json(sys.argv[1]).select("asin", "price")
reviews_df = spark.read.json(sys.argv[2]).select("asin", "overall")

meta = meta_df.rdd.map(lambda x: (x[0], x[1])).filter(lambda x: x[1] != None)
reviews = (
    reviews_df.rdd.map(lambda x: (x[0], (1, x[1])))
    .reduceByKey(lambda x, y: tuple([a + b for a, b in zip(x, y)]))
    .map(lambda x: (x[0], (x[1][0], x[1][1] / x[1][0])))
)

overall_top10 = reviews.join(meta).top(10, key=lambda x: x[1][0][0])
spark.sparkContext.parallelize(overall_top10).repartition(1).saveAsTextFile(sys.argv[3])

spark.stop()
