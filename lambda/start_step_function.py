import json
import boto3
import os

def lambda_handler(event, context):

    '''
    This function start AWS Step Functions
    '''
    
    bucket_name = event["Records"][0]['s3']['bucket']['name'] 
    bucket_arn = event["Records"][0]['s3']['bucket']['arn']
    key_name = event["Records"][0]['s3']['object']['key']
    file_name = event["Records"][0]['s3']['object']['key'].split('/')[-1]
    step_function_input = {}
    step_function_input['bucket_name'] = bucket_name
    step_function_input['bucket_arn'] = bucket_arn
    step_function_input['key_name'] = key_name
    step_function_input['file_name'] = key_name
    step_function_input['file_name'] = file_name
    print(step_function_input)
    print(key_name)
    client = boto3.client('stepfunctions')
    response = client.start_execution(stateMachineArn = os.environ['STEP_FUNC_ARN'],input= json.dumps(step_function_input))