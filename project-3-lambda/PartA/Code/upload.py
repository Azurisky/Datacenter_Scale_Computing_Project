import sys
import boto3
import time
import datetime

s3 = boto3.resource('s3').Bucket(sys.argv[1])
s3.objects.all().delete() #incase bucket is not empty

filenames_first = []
print("Uploading first batch...", str(datetime.datetime.now()))
for i in range (1, 601):
	filenames_first.append(str(i)+".txt")
for fn in filenames_first:
	s3.upload_file("tweets/"+fn, fn)
	time.sleep(1) #upload 1 file each sec for first 10mins
print("Uploaded first batch...", str(datetime.datetime.now()))

print("Uploading second batch...", str(datetime.datetime.now()))
filenames_second = []
for i in range (601, 801):
	filenames_second.append(str(i)+".txt")
for fn in filenames_second:
	s3.upload_file("tweets/"+fn, fn) #upload 200 files
print("Uploaded second batch...", str(datetime.datetime.now()))

print("Uploading third batch...", str(datetime.datetime.now()))
filenames_third = []
for i in range (801, 2001):
	filenames_third.append(str(i)+".txt")
for fn in filenames_third:
	s3.upload_file("tweets/"+fn, fn)
	time.sleep(1) #upload 1 file each sec for last 20mins
print("Uploaded third batch...", str(datetime.datetime.now()))