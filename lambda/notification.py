import json
import boto3
import os

def lambda_handler(event, context):
    '''
    This function sends the success/error notification
    '''
    print(event)
    client = boto3.client('sns')
    response = client.publish(
        TargetArn=os.environ['SNS_TOPIC'],
        Message=json.dumps({'default': json.dumps(event)}),
        MessageStructure='json'
    )