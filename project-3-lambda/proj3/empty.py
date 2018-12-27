import sys
import boto3
import time
s3 = boto3.resource('s3')
bucket = s3.Bucket(sys.argv[1])
bucket.objects.all().delete()