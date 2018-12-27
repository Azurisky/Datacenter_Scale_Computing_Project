docker network create spark-net
docker run -it --name spark-master ubuntu /bin/bash
apt update
apt install openjdk-8-jdk wget python

cd home
wget http://apache.claz.org/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz
tar xzf spark-2.3.2-bin-hadoop2.7.tgz
cd spark-2.3.2-bin-hadoop2.7
bin/pyspark --master local[2] --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 --conf "spark.mongodb.input.uri=mongodb://student:student@ec2-54-173-174-196.compute-1.amazonaws.com/test"


import pyspark.sql.functions as func

df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-173-174-196.compute-1.amazonaws.com/test.reviews").load()
df2 = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-173-174-196.compute-1.amazonaws.com/test.metadata").load()

item_review_counts = df1.groupBy("asin").agg(func.sum("overall").alias("rating_sum"), func.count(func.lit(1)).alias("counts"))
item_review_counts = item_review_counts.filter(item_review_counts["counts"] >= 100)
item_review_counts = item_review_counts.withColumn("rating_avg", func.col("rating_sum") / func.col("counts")).drop('rating_sum')
item_title_categories = df2.select(["asin", "categories", "title"])


output_cat_grouped = item_title_categories.join(item_review_counts,"asin", "inner").drop('asin')

# output_cat_grouped.filter(func.size("categories") > 1).take(10)
# B0000JML7G, B000654ZJ6, B000A2XBFE, B0000CEB61, B000AYEI9A, B000621452
output_cat = output_cat_grouped.withColumn("categories", func.explode(output_cat_grouped["categories"]))
# output_cat.take(10)

output = output_cat.collect()
output.sort(key = lambda x : x.categories)
output_lines = map(lambda x : "%s\t%s\t%d\t%f" % (x.categories, x.title, x.counts, x.rating_avg), output)
