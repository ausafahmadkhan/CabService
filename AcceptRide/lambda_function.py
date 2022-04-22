import json
import redis
import urllib3
import boto3


r = redis.Redis(host= 'redis-12008.c85.us-east-1-2.ec2.cloud.redislabs.com',port= '12008', username= "ausaf", password="Drogon@23")

client = boto3.client('apigatewaymanagementapi', endpoint_url= 'https://cgfkooghv6.execute-api.us-east-1.amazonaws.com/prod')

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
