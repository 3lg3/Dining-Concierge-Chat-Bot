import json
import boto3

def encoding(text):
    unstructured={}
    unstructured['text'] = text
    unstructured['id'] = 'id1'
    unstructured['timestamp'] = ''
    
    message = {}
    message['type'] = 'unstructured'
    message['unstructured'] = unstructured
    
    chatResponse = {}
    chatResponse['messages'] = []
    chatResponse['messages'].append(message)
    
    print(chatResponse)

    responseObject = {}
    responseObject['statusCode'] = 200
    responseObject['header'] = {}
    responseObject['header']['Content-Type'] = 'application/json'
    responseObject['body'] = chatResponse
    
    return responseObject
    
def decode(event):
    return event['messages'][0]['unstructured']['text']

def lambda_handler(event, context):
    # get frontend message
    ## decode to get the text
    text = decode(event)
   
    botClient = boto3.client('lex-runtime')
    
    lexResponse = botClient.post_text(
        botName="restaurantSuggestion",
        botAlias="rest",
        userId="eleven11Li",
        sessionAttributes={},
        requestAttributes={},
        inputText=text,
        activeContexts=[]
    )
    
    
    # return the encoded message
    return encoding(lexResponse['message'])