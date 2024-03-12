from flask import Flask, request
import csv
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, EndpointConnectionError
# from line_profiler import LineProfiler

app=Flask(__name__)

csv_file_path='Classification_Results.csv'

Results_dict={}

req_queue_url='https://sqs.us-east-1.amazonaws.com/637423171832/1228052438-req-queue'
resp_queue_url='https://sqs.us-east-1.amazonaws.com/637423171832/1228052438-resp-queue'

s3=boto3.client('s3')
sqs=boto3.client('sqs')

def sendMessageToReqQueue(s3_file_path):
    message_body=s3_file_path
    print(s3_file_path)
    response = sqs.send_message(
        QueueUrl=req_queue_url,
        MessageBody=message_body
    )
    print(f"Message sent to request queue successfully. Message ID: {response['MessageId']}, Message: {response}")


def uploadToS3(file_object,bucket_name,s3_file_name):
    
    try:
        s3.upload_fileobj(file_object,bucket_name,s3_file_name)
        s3_file_path=s3_file_name
        print(f'File uploaded to S3 Successfully to {s3_file_path}')
        sendMessageToReqQueue(s3_file_path)
    except NoCredentialsError:
        print('Credentials Not Available')

def getNameFromFile(csv_file,target_file_name):
    if not bool(Results_dict):
        with open(csv_file,'r') as file:
            reader=csv.DictReader(file)
            for row in reader:
                Results_dict[row['Image']]=row['Results']
    if(Results_dict[target_file_name]):
        return Results_dict[target_file_name]
    return "Not Found"

# check how this works for concurrent requests
def getResponseFromRespQueue():
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=resp_queue_url,
                AttributeNames=[
                    'All'
                ],
                MessageAttributeNames=[
                    'All'
                ],
                MaxNumberOfMessages=1,
                VisibilityTimeout=900,
                WaitTimeSeconds=20
            )
    
            messages = response.get('Messages',[])
    
            if messages:
                for message in messages:
                    print(f"Received message: Message body {message['Body']}")
                    result=message['Body']
    
                    receipt_handle=message['ReceiptHandle']

                    # response = sqs.get_queue_attributes(
                    #     QueueUrl=resp_queue_url,
                    #     AttributeNames=['ApproximateNumberOfMessages']
                    # )
                    # print(int(response['Attributes']['ApproximateNumberOfMessages']));

                    sqs.delete_message(
                        QueueUrl=resp_queue_url,
                        ReceiptHandle=receipt_handle
                    )

                    # response = sqs.get_queue_attributes(
                    #     QueueUrl=resp_queue_url,
                    #     AttributeNames=['ApproximateNumberOfMessages']
                    # )
                    # print(int(response['Attributes']['ApproximateNumberOfMessages']));
            else:
                print("No messages received. Waiting for messages...")
            
            return result
        
        except NoCredentialsError:
            print("Credentials not available. Please provide valid AWS credentials.")
        except PartialCredentialsError:
            print("Partial credentials provided. Please provide valid AWS credentials.")
        except EndpointConnectionError:
            print("Error connecting to the SQS endpoint. Please check your network connectivity or endpoint configuration.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")  

        

# @lp.profile
@app.route('/',methods=['POST'])
def post_data():
    if 'inputFile' not in request.files:
        return 'No File Uploaded, please upload!'

    file=request.files['inputFile']
    # fileName, extension=os.path.splitext(file.filename)
    s3_key=file.filename

    #Uploading the file to S3 input bucket
    uploadToS3(file,"1228052438-in-bucket",s3_key)

    
    response = getResponseFromRespQueue()

    print('Here')
    print(response)

    # sqs.delete_message(
    #     QueueUrl=resp_queue_url,
    #     ReceiptHandle=receipt_handle
    # )
    
    #change this acc to part2
    # result=getNameFromFile(csv_file_path,fileName)
    # response_data=fileName+':'+result
    # response_data=response_data.replace('"','')
    # response_data=response_data.strip()
    # if response:
    return response

# lp.add_profile(post_data)

def main():
    app.run(host='0.0.0.0', port=80, threaded=True)
     
if __name__=='__main__':
#    lp(post_data)()
#    lp.print_stats()
   main()
   