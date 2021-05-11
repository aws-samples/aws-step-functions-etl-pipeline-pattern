import json
import boto3
import time
import cfnresponse
import os

def lambda_handler(event, context):
    '''
    This function starts AWS codebuild project
    '''
    print("started")
    response_data = {}
    the_event = event['RequestType']
    if the_event in ('Create', 'Update'):
        try:            
            print("The event is: ", str(the_event))
            counter = 0
            client = boto3.client(service_name='codebuild')
            buildSucceeded = False
            Update_lambda_layer = event['ResourceProperties']['Update_lambda_layer']
            
            if Update_lambda_layer == "yes":
                code_build_project_name = str(os.environ['PROJECT_NAME'])
                new_build = client.start_build(projectName=code_build_project_name)
                buildId = new_build['build']['id']
                while counter < 50:  
                    time.sleep(5)
                    print("wating...")
                    counter = counter + 1
                    theBuild = client.batch_get_builds(ids=[buildId])
                    buildStatus = theBuild['builds'][0]['buildStatus']

                    if buildStatus == 'SUCCEEDED':
                        buildSucceeded = True
                        print("Build Success")
                        time.sleep(15)
                        cfnresponse.send(event,context,cfnresponse.SUCCESS,response_data)
                        break
                    elif buildStatus == 'FAILED' or buildStatus == 'FAULT' or buildStatus == 'STOPPED' or buildStatus == 'TIMED_OUT':
                        response_data['Data'] = buildStatus
                        cfnresponse.send(event,context,cfnresponse.FAILED,response_data)
                        break
            else:
              response_data['Data'] = "No update needed"
              cfnresponse.send(event,context,cfnresponse.SUCCESS,response_data)

        except Exception as e:
            print("Execution failed...")
            print(str(e))
            response_data['Data'] = str(e)
            cfnresponse.send(event,context,cfnresponse.FAILED,response_data)
    else:
      cfnresponse.send(event,context,cfnresponse.SUCCESS,response_data)