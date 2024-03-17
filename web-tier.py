from io import BytesIO
from flask import Flask, request
import csv
import os
import json
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
import threading
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, EndpointConnectionError

app=Flask(__name__)



#local store for storing messages from response queue
results_dict=dict()

req_queue_url='https://sqs.us-east-1.amazonaws.com/905418031675/1228052438-req-queue'
resp_queue_url='https://sqs.us-east-1.amazonaws.com/905418031675/1228052438-resp-queue'

s3_in_bucket="1228052438-in-bucket-1"

s3=boto3.client('s3',region_name="us-east-1")
sqs=boto3.client('sqs',region_name="us-east-1")

def get_response_once_available(file_name):
    try:
        while file_name not in results_dict.keys():
            continue
        face_name = results_dict[file_name]
        results_dict.pop(file_name)
        return face_name
    except:
        print("Error while waiting for response from Queue")

def sendMessageToReqQueue(image_byte_array):
    try:
        response = sqs.send_message(
            QueueUrl=req_queue_url,
            MessageBody=image_byte_array
        )
        print(f"Message sent to request queue successfully. Message ID: {response['MessageId']}")
        
    except NoCredentialsError:
        print("Credentials not available. Please provide valid AWS credentials.")
    except PartialCredentialsError:
        print("Partial credentials provided. Please provide valid AWS credentials.")
    except EndpointConnectionError:
        print("Error connecting to the SQS endpoint. Please check your network connectivity or endpoint configuration.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# check how this works for concurrent requests
def getResponseFromRespQueue(): #fix the return of this
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
                MaxNumberOfMessages=10,
                VisibilityTimeout=10,
                WaitTimeSeconds=2
            )
    
            messages = response.get('Messages',[])
    
            if messages:
                for message in messages:
                    print(f"Received message: Message body {message['Body']}")
                    result=message['Body']
                    file_name,face_name=result.split(':')
                    results_dict[file_name]=result
                    
                    receipt_handle=message['ReceiptHandle']
                    sqs.delete_message(
                        QueueUrl=resp_queue_url,
                        ReceiptHandle=receipt_handle
                    )
            # else:
                # print("No messages received. Waiting for messages...")
        
        except NoCredentialsError:
            print("Credentials not available. Please provide valid AWS credentials.")
        except PartialCredentialsError:
            print("Partial credentials provided. Please provide valid AWS credentials.")
        except EndpointConnectionError:
            print("Error connecting to the SQS endpoint. Please check your network connectivity or endpoint configuration.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")  


# Establishing an independent thread to manage ongoing polling from the Response Queue.
sqs_monitoring_thread = threading.Thread(target=getResponseFromRespQueue)
sqs_monitoring_thread.start()

        
# num_max_workers = 100
@app.route('/',methods=['POST'])
def post_data():
    if 'inputFile' not in request.files:
        return 'No File Uploaded, please upload!'

    file=request.files['inputFile']

    # sending image as byte array to sqs request queue
    image_bytes = file.read()
    
    message = {
        "key": file.filename,
        "value": image_bytes.decode('latin-1')
    }


    sendMessageToReqQueue(json.dumps(message))

    file_name, extension=os.path.splitext(file.filename)

    response = get_response_once_available(file_name)

    return response

    # with ThreadPoolExecutor(max_workers = num_max_workers) as executor:
    #     response = getResponseFromRespQueue(s3_file_name)
    #     print("returning - ", response)
     #check this
    
def main():
    app.run(host='0.0.0.0', port=8000, threaded=True)
     
if __name__=='__main__':
   main()
   