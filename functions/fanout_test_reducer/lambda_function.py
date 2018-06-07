# coding: utf-8
from __future__ import print_function

import boto3
import uuid
import json
from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    job_id = message['job_id']
    print(job_id)
    table = dynamodb.Table('fanout-test-compute-results')
    response = table.query(
        KeyConditionExpression=Key('job_id').eq(job_id)
    )
    items = response['Items']
    sum = 0
    for i in items:
        sum += i['result']

    print(items)  
    print("Finished reducer. sum={} , job_id={}".format(sum, job_id))
    return json.dumps({'sum':str(sum)})

