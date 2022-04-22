import json
import redis
import urllib3
import boto3


r = redis.Redis(host= 'redis-host',port= '6379', username= "user", password="pwd")

client = boto3.client('apigatewaymanagementapi', endpoint_url= 'https://abc/stage')

def lambda_handler(event, context):
    connectionId = event['requestContext']['connectionId']
    body = json.loads(event['body'])
    lat = body['fromLatitude']
    long = body['fromLongitude']

    completeRide(connectionId, lat, long)
    return {
        'statusCode': 200
    }


def completeRide(connectionId, lat, long):
    try:
        passengerConnectId = r.get(connectionId)
        passengerConnectId = passengerConnectId.decode()
        if r.get(passengerConnectId +'_trip'):
            client.post_to_connection(ConnectionId=passengerConnectId, Data=json.dumps('Ride complete.. Thank you for choosing our cab service!').encode('utf-8'))     
            r.set(passengerConnectId +'_trip', 'complete')
            r.geoadd("availableDrivers", [long, lat, connectionId], True, False)
            r.delete(connectionId)
            
        
    except Exception as e:
        print(e)
