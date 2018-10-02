wget https://s3.amazonaws.com/datacenter-andrew-rishitha/PartC/buys.txt
mkdir input
mv buys.txt input/
hadoop fs -put input/
hadoop jar ../TimeBlocks.jar TimeBlocks ./input/buys.txt ./output_timeblocks
hadoop fs -get output_timeblocks/