import json
import boto3
import requests
from requests_aws4auth import AWS4Auth

# URL for Elastic Search with size = 3 and queried cuisine
ES_SEARCH_URL = 'https://search-restaurants-qwj66xmt3m5nnscivcr3kpytfq.us-east-1.es.amazonaws.com/_search?size=3&&q=cuisine:' 

# Authorization grant
# credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-east-1', 'es', session_token=credentials.token)


def pollMessage():
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/905863309174/diningSqs'
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=["test"],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=2,
        WaitTimeSeconds=2
    )
    if "Messages" in response.keys(): 
        message = response["Messages"][0]
        receipt_handle = message['ReceiptHandle']
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        return message["MessageAttributes"]
    else:
        return None

# Use elastic search to collect restaurants id in the cuisine
def collect_restaurants_id(url, cuisine):
    url += cuisine
    response = requests.get(url, auth=("master", "password")).json()
    results = response['hits']['hits']
    rest_id = []
    for result in results:
        rest_id.append(result["_source"]["id"])
    
    return rest_id

# Use Dynamodb to get the restaurant information (name, address...) for each id

def get_restaurant_info(rest_id):
    db = boto3.resource('dynamodb')
    table = db.Table('yelp-restaurants')
    rest_info = []   #list of 
    for id in rest_id:
        curr = table.get_item(Key={'id': id})['Item']
        rest_info.append(curr)
    return rest_info
    
# send the message to be sent according to the restaurant info list
def send_message(rest_info, phone, cuisine, numberofPeople, diningTime, email):
    client = boto3.client('sns')
    client.set_sms_attributes(attributes={'DefaultSMSType': 'Transactional'})
    if not phone.startswith('+'):
        phone = '+1'+phone
    msg = 'Hello! Here are my ' + cuisine
    msg += ' ' + 'restaurant suggestions for ' + numberofPeople + ' people'
    msg += ', for today at {}: \n'.format(diningTime)
    count = 1
    for row in rest_info:
        msg += '{}. {}, located at {}\n'.format(count, row['name'], row['address'])
        count += 1
        
    response = client.publish(Message=msg, PhoneNumber=phone)
    
    
    ses = boto3.client('ses')
    ses.send_email(
        Source='zl3029@columbia.edu',
        Destination={
            'ToAddresses': [email],
        },
        Message={
            'Subject': {
                'Data': 'Dining suggestions',
                'Charset': "UTF-8"
            },
            'Body':{
                'Text': {
                    'Data': msg,
                    'Charset': "UTF-8"
                }
            }
        }
    )
    return response
    
# send the message with not correct info
def send_message_wrong(phone, text):
    client = boto3.client('sns')
    client.set_sms_attributes(attributes={'DefaultSMSType': 'Transactional'})
    if not phone.startswith('+'):
        phone = '+1'+phone
    response = client.publish(Message=text, PhoneNumber=phone)
    return response
    

def lambda_handler(event, context):
    # Collect info (polling) from queue
    info = pollMessage()
    print("SQS poll: ")
    print(info)
    if info is not None:
        phone = info['phone']['StringValue']
        numberOfDigits = sum(n.isdigit() for n in phone)
        if numberOfDigits != 10:
            if numberOfDigits != 11 or phone[0] != '+':
                print(phone + " is wrong number")
                return None
                 
        numberofPeople = info['numberofPeople']['StringValue']
        diningTime = info['diningTime']['StringValue']
        email = info['email']['StringValue']
        
        location = info['location']['StringValue']
        print(location)
        location_list = ['Manhattan', 'manhattan', 'New York', 'new york']
        if location not in location_list:
            print("city error, not match Manhattan")
            response = send_message_wrong(phone, "We cannot find your location, please try other city(Manhattan)")
            return None
        
        cuisine = info['cuisine']['StringValue']
        rest_id = collect_restaurants_id(ES_SEARCH_URL, cuisine)
        if not rest_id:
            print("cuisine error, cannot find the cuisine in the es search")
            response = send_message_wrong(phone, "We cannot find your target cuisine, please try other one")
            return None
        
        rest_info = get_restaurant_info(rest_id)
        print("send message with recommendation")
        response = send_message(rest_info, phone, cuisine, numberofPeople, diningTime, email)
        return None
    
    return None

