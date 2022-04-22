import json
import redis
import urllib3
import boto3


r = redis.Redis(host= 'redis-host',port= '6379', username= "user", password="pwd")

client = boto3.client('apigatewaymanagementapi', endpoint_url= 'https://abc/stage')

def lambda_handler(event, context):
    connectionId = event['requestContext']['connectionId']
    acceptRide(connectionId)
    return {
        'statusCode': 200
    }


def acceptRide(connectionId):
    try:
        passengerConnectId = r.get(connectionId)
        passengerConnectId = passengerConnectId.decode()
        tripStatus = r.get(passengerConnectId +'_trip')
        if tripStatus is None or tripStatus.decode() == 'complete':
            r.set(passengerConnectId +'_trip', 'accepted')
            client.post_to_connection(ConnectionId=passengerConnectId, Data=json.dumps('Ride accepted.. Driver is coming to your pickup location').encode('utf-8')) 
            r.zrem('availableDrivers', connectionId)
        else:
            r.delete(connectionId)
            client.post_to_connection(ConnectionId=connectionId, Data=json.dumps('Ride already assigned to a different driver.').encode('utf-8'))     
            
    except Exception as e:
        print(e)
