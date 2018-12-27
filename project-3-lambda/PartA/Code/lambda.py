from __future__ import print_function
import boto3
import json
import time
import os
import sys
import uuid
import urllib

s3_client = boto3.client('s3')

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

def removeNoise(word):
    ans = ""
    for i in word:
        if i.isalpha():
            ans += i
    return ans

def lambda_handler(event, context):
    # for record in event['Records']:
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key'] 
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
    upload_path = '/tmp/output-{}'.format(key)
    
    s3_client.download_file(bucket, key, download_path)
    with open(upload_path,'w') as new_file:
        with open(download_path) as fp:
            for index, line in enumerate(fp):
                print(line)
                buf = []
                words = line.split()
                for word in words:
                    new = removeNoise(word).lower()
                    if new and new not in stop_words:
                        buf.append(new)
                print(" ".join(buf) + "\n")
            new_file.write(" ".join(buf) + "\n")
    s3_client.upload_file(upload_path, '{}-output'.format(bucket), key)




