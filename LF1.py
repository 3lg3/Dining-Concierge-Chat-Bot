import json
import boto3

def sendInfoToSQS(infos):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/905863309174/diningSqs'
    
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=10,
        MessageAttributes={
            'location': {
                'DataType': 'String',
                'StringValue': infos["Location"]
            },
            'cuisine': {
                'DataType': 'String',
                'StringValue': infos["Cuisine"]
            },
            'diningTime': {
                'DataType': 'String',
                'StringValue': infos["DiningTime"]
            },
            'numberofPeople': {
                'DataType': 'String',
                'StringValue': infos["NumberPeople"]
            },
            'phone': {
                'DataType': 'String',
                'StringValue': infos["Phone"]
            },
            'email': {
                'DataType': 'String',
                'StringValue': infos['Email']
            }
        },
        MessageBody=("sending data")
    )
    
    return response

def lambda_handler(event, context):
    # TODO implement
    intent = event["currentIntent"]["name"]
    if intent == "DiningSuggestion":
        slots = event["currentIntent"]["slots"]
        location = slots["Location"]
        cuisine = slots["Cuisine"]
        diningTime = slots["DiningTime"]
        number = slots["NumberPeople"]
        phone = slots["Phone"]
        
        sqsResponse = sendInfoToSQS(slots)
        print(sqsResponse)

        text = "I have successfully collection your information. You want have {} in {} at {} with {} people! I will notify you over SMS with Phone number {}".format(cuisine, location, diningTime, number, phone)

        return {
        "dialogAction":{
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message":{
                "contentType": "PlainText",
                "content": text
                }
            }
        }
