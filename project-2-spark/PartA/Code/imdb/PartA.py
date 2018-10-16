from pyspark import SparkContext
sc =SparkContext()

data = sc.textFile("quotes.list")

stop_word = set(["a","about","above","after","again",
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

def writeFile(data):
    f = open("output.txt", 'w')
    tmp = float('inf')
    List = []
    for i, j in data:
        if j == tmp:
            List.append(i)
        elif j < tmp:
            List.sort()
            for k in List:
                f.write('{}\t{}\n'.format(k, tmp))
            tmp = j
            List = [i]
    if List:
        List.sort()
        for k in List:
            f.write('{}\t{}\n'.format(k, tmp))

def removeNoise(word):
    ans = ""
    for i in word:
        if i.isalpha():
            ans += i
    return ans

def mapper(word):
    word = removeNoise(word)
    word = word.lower()
    if word and word not in stop_word:
        return (word, 1)
    return (word, 0)

counts = data.flatMap(lambda line: line.split()).map(lambda word: mapper(word)).reduceByKey(lambda v1,v2: v1 + v2).map(lambda (a, b): (b, a)).sortByKey(0, 1).map(lambda (a, b): (b, a))
# counts.coalesce(1, True).saveAsTextFile("/home/spark-2.3.2-bin-hadoop2.7/output")
output = counts.takeOrdered(2000, lambda x : (-x[1], x[0]))
writeFile(output)
