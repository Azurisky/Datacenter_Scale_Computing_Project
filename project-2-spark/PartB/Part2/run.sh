mkdir -p Bucket{1,2,3,4}
spark-submit --executor-cores 3 --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 run.py
