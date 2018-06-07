# coding: utf-8
#from __future__ import print_function

import boto3
import uuid
import json
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

TOPIC_ARN = 'arn:aws:sns:us-east-1:YOU_ACCOUNT_ID:fanout-test-map'

def lambda_handler(event, context):
    max_jobs = 10
    job_id = str(uuid.uuid4())
    
    # write
    table = dynamodb.Table('fanout-test-jobs')
    response = table.put_item(
        Item={
            'job_id': job_id,
            'task_id': job_id,
            'jobs_left': max_jobs
        }
    )
    
    for i in range(max_jobs):
        message = json.dumps({
            'job_id': job_id,
            'compute_value': i
        })
        print(message)

        response = sns.publish(
            TopicArn=TOPIC_ARN,
            Message=message
        )
        print(response)
    

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {},
        "body": "Mapper Complete"
    }
