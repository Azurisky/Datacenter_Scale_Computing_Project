wget https://s3.amazonaws.com/datacenter-andrew-rishitha/imdb/quotes.list
hadoop fs -put quotes.list
spark-submit --executor-cores 3 --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 PartA.py