# coding: utf-8
from __future__ import print_function

import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

REDUCE_TOPIC_ARN = 'arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:fanout-test-reduce'

def lambda_handler(event, context):
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    job_id = message['job_id']
    compute_value = message['compute_value']
    print("Running compute for job_id:{} , compute_value:{}".format(job_id, compute_value))
    
    compute_and_write(job_id, context.aws_request_id, compute_value)
    
    decrement_job(job_id, job_id)
    return 'done'

def decrement_job(job_id, task_id):
    table = dynamodb.Table('fanout-test-jobs')
    response = table.update_item(
        Key={
            'job_id': job_id,
            'task_id': task_id
        },
        UpdateExpression="set jobs_left = jobs_left - :val",
        ExpressionAttributeValues={
            ':val': decimal.Decimal(1)
        },
        ReturnValues="UPDATED_NEW"
    )
    jobs_left = response['Attributes']['jobs_left']
    if jobs_left <= 0:
        notify_reduce(job_id)
        
def notify_reduce(job_id):
    message = json.dumps({
        'job_id': job_id
    })
    print("last job. Notifying reduce: {}".format(message))
    response = sns.publish(
        TopicArn=REDUCE_TOPIC_ARN,
        Message=message
    )
    print(response)

def compute_and_write(job_id, task_id, compute_value):
    # compute
    result_value = int(compute_value) * 2
    
    # write
    table = dynamodb.Table('fanout-test-compute-results')
    response = table.put_item(
        Item={
            'job_id': job_id,
            'task_id': task_id,
            'result': result_value
        }
    )

