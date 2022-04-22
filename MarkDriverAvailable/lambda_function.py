import json
import redis
import urllib3
import boto3


r = redis.Redis(host= 'redis-12008.c85.us-east-1-2.ec2.cloud.redislabs.com',port= '12008', username= "ausaf", password="Drogon@23")

client = boto3.client('apigatewaymanagementapi', endpoint_url= 'https://cgfkooghv6.execute-api.us-east-1.amazonaws.com/prod')

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
