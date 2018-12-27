Generally speaking, I follow the instruction on aws website to setup the Lambda function. 

First, I created a role with access for Lambda execution, and then create two buckets in S3. After that, I just need to put my "lambda.py" into function in AWS Lambda page, and set the configuration which made S3 as input.

In the end, we can upload the files into source S3 bucket, every items uploaded will trigger the lambda function and then copy the modified file into target S3 bucket.