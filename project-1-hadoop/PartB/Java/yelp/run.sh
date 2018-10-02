hadoop jar ../WordCountLarge.jar WordCountLarge s3://wordcount-datasets/ ./output_yelp
hadoop fs -get output_yelp/