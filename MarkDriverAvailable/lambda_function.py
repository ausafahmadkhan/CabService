import json
import redis
import urllib3
import boto3


r = redis.Redis(host= 'redis-host',port= '6379', username= "user", password="pwd")

client = boto3.client('apigatewaymanagementapi', endpoint_url= 'https://abc/stage')

def lambda_handler(event, context):
    body = json.loads(event['body'])
    connectionId = event['requestContext']['connectionId']
    lat = body['fromLatitude']
    long = body['fromLongitude']


    markDriverAvailable(connectionId, lat, long)
    postMessage(connectionId, 'Ready to accept rides...')    
    
    return {
        'statusCode': 200
    }


def markDriverAvailable(connectionId, lat, long):
    try:
        r.geoadd("availableDrivers", [long, lat, connectionId], True, False)
    except Exception as e:
        print(e)


def postMessage(connectionId, message):
    client.post_to_connection(ConnectionId=connectionId, Data=json.dumps(message).encode('utf-8')) 
