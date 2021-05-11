import json
import boto3

def lambda_handler(event, context):

    '''
    This function start AWS Glue Crawler
    it expects 'Crawler_name' key in the event object passed in the function
    '''

    Crawler_Name=event['Crawler_Name']
    client = boto3.client('glue')
    print("Checking crawler status")
    response = client.start_crawler(Name=Crawler_Name)
    result={}
    result['crawler_name']=Crawler_Name
    return(result)