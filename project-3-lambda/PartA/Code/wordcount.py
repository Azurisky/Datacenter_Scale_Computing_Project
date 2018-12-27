import os
import collections

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

ch = [chr(ord('a') + i) for i in range(26)]

def removeNoise(word):
    ans = ""
    for i in word:
        if i in ch:
            ans += i
    return ans

word_dic = {}
for root, dirs, files in os.walk('output/'):
    for file in files:
        # print(file)
        if file.endswith(".txt"):
            with open('output/{}'.format(file)) as fp:
                for line in fp:
                    words = line.split()
                    for word in words:
                        # word = removeNoise(word.lower())
                        # if word and word not in stop_words:
                        word_dic[word] = word_dic.get(word, 0)
                        word_dic[word] += 1

num_dic = {}
for word, num in word_dic.items():
    num_dic[num] = num_dic.get(num, [])
    num_dic[num].append(word)

with open('wordcount.txt','w') as new_file:
    for num in sorted(num_dic, reverse=True):
        l = num_dic[num]
        for word in sorted(l):
            new_file.write('{}\t{}\n'.format(word, num))


