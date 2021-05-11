import json
import boto3
import botocore
import os

def lambda_handler(event, context):  

    '''
    This function Moves the source dataset to archive/transform/error folder
    '''
    print(event)
    s3_resource = boto3.resource('s3')
    bucket_name = event['bucket_name']
    result = {}
    if "error-info" in event:
        source_location = "stage"
        status = "FAILURE"
    else:
        source_location = event['taskresult']['Location']
        status = event['taskresult']['Validation']

    file_name = event['file_name']
    key_name = source_location + "/" + file_name

    if status == "FAILURE":
        print("Status is set to failure. Moving to error folder")
        folder = os.environ['error_folder_name']
    elif status == "SUCCESS":
        print("Status is set to archive. Moving to archive folder")
        folder = os.environ['archive_folder_name']      
    source_file_name_to_copy = bucket_name + "/" + source_location + "/" + file_name
    move_file_name= folder + "/" + file_name
    print("moving file to " + move_file_name)
    s3_resource.Object(bucket_name, move_file_name).copy_from(CopySource=source_file_name_to_copy)
    s3_resource.Object(bucket_name, key_name).delete()
    result['Status'] = status
    result['msg'] = "File moved to " + move_file_name
    return(result)