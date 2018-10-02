wget https://s3.amazonaws.com/datacenter-andrew-rishitha/PartC/clicks.txt
mkdir input
mv clicks.txt input/
hadoop fs -put input/
hadoop jar ../ItemClick.jar ItemClick ./input/clicks.txt ./output_itemclick
hadoop fs -get output_itemclick/