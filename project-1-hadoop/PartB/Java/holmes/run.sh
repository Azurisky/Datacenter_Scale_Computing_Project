wget https://norvig.com/big.txt
mkdir input_holmes
mv big.txt input_holmes/
hadoop fs -put input_holmes/
hadoop jar ../WordCount.jar WordCount ./input_holmes ./output_holmes
hadoop fs -get output_holmes/