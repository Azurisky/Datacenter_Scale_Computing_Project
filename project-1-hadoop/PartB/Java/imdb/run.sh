wget http://ftp.sunet.se/mirror/archive/ftp.sunet.se/pub/tv+movies/imdb/quotes.list.gz
gzip -d quotes.list.gz
mkdir input_imdb
mv quotes.list input_imdb/
hadoop fs -put input_imdb/
hadoop jar ../WordCount.jar WordCount ./input_imdb ./output_imdb
hadoop fs -get output_imdb/