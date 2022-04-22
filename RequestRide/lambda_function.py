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


    requestRide(connectionId, lat, long)
    updateDrivers(long, lat, connectionId)
    return {
        'statusCode': 200
    }

def requestRide(connectionId, lat, long):

    postMessage(connectionId, 'Looking for drivers near you...')

    try:
        r.geoadd("waitingPassengers", [long, lat, connectionId], True, False)
    except Exception as e:
        print(e)

def updateDrivers(long, lat, connectionId):
    try:
        drivers = r.georadius("availableDrivers", float(long), float(lat), 8, "km")
        for i in drivers :
            postMessage(i.decode(),'Got a ride request for you..Please accept!')
            r.setex(i.decode(), 60 * 60 * 24, str(connectionId))
    except Exception as e :
        print(e)


def postMessage(connectionId, message):
    client.post_to_connection(ConnectionId=connectionId, Data=json.dumps(message).encode('utf-8')) 
