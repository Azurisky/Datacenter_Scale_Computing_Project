wget https://s3.amazonaws.com/datacenter-andrew-rishitha/PartC/buys.txt
wget https://s3.amazonaws.com/datacenter-andrew-rishitha/PartC/clicks.txt
mkdir input
mv clicks.txt input/
mv buys.txt input/
hadoop fs -put input/
hadoop jar ../SuccessRate.jar SuccessRate ./input ./output_successrate
hadoop fs -get output_successrate/