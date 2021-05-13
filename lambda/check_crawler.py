import os
import boto3


def lambda_handler(event, context):
    '''
    This function checks the status of the Crawler
    it expects 'crawler_name' key in the event object passed in the function
    '''
    print(event)
    Crawler_Name = event['crawler_name']
    cnt = int(event['cnt'])+1
    result = {}
    print("Crawler name is set to : " + str(Crawler_Name))
    client = boto3.client('glue')
    print("Checking crawler status")
    response = client.get_crawler(Name=Crawler_Name)
    print(response['Crawler']['State'])
    state = response['Crawler']['State']
    last_state = "INITIAL"
    if 'LastCrawl' in response['Crawler']:
        if 'Status' in response['Crawler']['LastCrawl']:
            last_state = response['Crawler']['LastCrawl']['Status']
    print(response)
    result['Status'] = response['Crawler']['State']
    result['Validation'] = "RUNNING"
    if state == "READY":
        result['Validation'] = "SUCCESS"
        if last_state == "FAILED":
            result['Status'] = "FAILED"
            result['error'] = "Crawler Failed"
            result['Validation'] = "FAILURE"
    Retry_Count = int(os.environ['RETRYLIMIT'])
    if cnt > Retry_Count:
        result['Status'] = "RETRYLIMITREACH"
        result['error'] = "Retry limit reach"
        result['Validation'] = "FAILURE"

    result['crawler_name'] = Crawler_Name
    result['running_time'] = response['Crawler']['CrawlElapsedTime']
    result['cnt'] = cnt
    result['last_crawl_status'] = last_state
    result['Location'] = "stage"
    return result
