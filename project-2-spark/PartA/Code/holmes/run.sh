wget https://s3.amazonaws.com/datacenter-andrew-rishitha/holmes/Data.txt
hadoop fs -put Data.txt
spark-submit --executor-cores 3 --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 PartA.py