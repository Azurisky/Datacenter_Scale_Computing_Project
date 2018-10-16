from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()
df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews").load()

def transform_string(string):
    stop_words = set(["a","about","above","after","again",
        "against","all","am","an","and","any","are","arent","as","at","be","because","been","before",
        "being","below","between","both","but","by","cant","cannot","could","couldnt","did","didnt","do",
        "does","doesnt","doing","dont","down","during","each","few","for","from","further","had",
        "hadnt","has","hasnt","have","havent","having","he","hed","hell","hes","her","here","heres","hers",
        "herself","him","himself","his","how","hows","i","id","ill","im","ive","if","in","into","is",
        "isnt","it","its","its","itself","lets","me","more","most","mustnt","my","myself","no","nor",
        "not","of","off","on","once","only","or","other","ought","our","ours", "ourselves","out","over","own",
        "same","shant","she","shed","shell","shes","should","shouldnt","so","some","such","than","that","thats",
        "the","their","theirs","them","themselves","then","there","theres","these","they","theyd","theyll","theyre",
        "theyve","this","those","through","to","too","under","until","up","very","was","wasnt","we","wed","well",
        "were","weve","were","werent","what","whats","when","whens","where","wheres","which","while","who","whos",
        "whom","why","whys","with","wont","would","wouldnt","you","youd","youll","youre","youve","your","yours",
        "yourself","yourselves"])
    string = ' '.join(string.split())
    lower_string = ''.join(c.lower() for c in string if c.isalpha() or c == ' ')
    simple_string = " ".join([x for x in lower_string.split() if x not in stop_words and len(x) > 0])
    return simple_string

ts_udf = udf(transform_string)

for i in range(1, 6):
    review_text_df = df1.filter(df1["overall"] == i*1.0).select(["reviewText"])
    review_text_tokens = review_text_df.withColumn("simple_string", ts_udf(df1["reviewText"])).drop("reviewText")
    tokens_df = review_text_tokens.withColumn("tokens", func.explode(func.split("simple_string", "\s+"))).drop("simple_string")
    tokens_count = tokens_df.groupBy("tokens").agg(func.count(tokens_df["tokens"])).alias("count")
    output = tokens_count.rdd.takeOrdered(500, lambda x: (-x[1], x[0]))
    with open("Bucket%d/output.txt" % i, "w") as f:
        for ii in output:
            f.write("%s\t%i\n" % (ii[0], ii[1]))
