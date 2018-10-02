hadoop jar ../WordCount.jar WordCount  s3://csci5253-gutenberg-dataset/ ./output_g
hadoop fs -get output_g/
cd output_g/
sort -k2 -n -r part-r-000* > output_tmp.txt
head -2000 output_tmp.txt > output.txt
