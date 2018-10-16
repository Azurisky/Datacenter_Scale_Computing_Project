import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews").load()
df2 = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.metadata").load()

item_review_counts = df1.groupBy("asin").agg(func.sum("overall").alias("rating_sum"), func.count(func.lit(1)).alias("counts"))
item_review_counts = item_review_counts.filter(item_review_counts["counts"] >= 100)
item_review_counts = item_review_counts.withColumn("rating_avg", func.col("rating_sum") / func.col("counts")).drop('rating_sum')
item_title_categories = df2.select(["asin", "categories", "title"])


output_cat_grouped = item_title_categories.join(item_review_counts, "asin", "inner").drop('asin')

output_cat = output_cat_grouped.withColumn("categories", func.explode(output_cat_grouped["categories"]))
output_cat_ranked = output_cat.withColumn("rank", func.dense_rank().over(Window.partitionBy("categories").orderBy(func.desc("rating_avg"))))
output_cat_top = output_cat_ranked.filter(output_cat_ranked["rank"] == 1)
output = output_cat_top.collect()
output.sort(key = lambda x : x.categories)
output_lines = map(lambda x : "%s\t%s\t%d\t%f" % (x.categories, x.title, x.counts, x.rating_avg), output)

with open("output.txt", "w") as f:
    for line in output_lines:
        f.write(line + '\n')
