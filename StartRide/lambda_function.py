import json
import redis
import urllib3
import boto3


r = redis.Redis(host= 'redis-host',port= '6379', username= "user", password="pwd")

client = boto3.client('apigatewaymanagementapi', endpoint_url= 'https://abc/stage')

def lambda_handler(event, context):
    connectionId = event['requestContext']['connectionId']
    startRide(connectionId)
    return {
        'statusCode': 200
    }


def startRide(connectionId):
    try:
        passengerConnectId = r.get(connectionId)
        passengerConnectId = passengerConnectId.decode()
        
        if r.get(passengerConnectId +'_trip'):
            client.post_to_connection(ConnectionId=passengerConnectId, Data=json.dumps('Starting the ride... Please provide OTP to the driver!').encode('utf-8'))     
            r.set(passengerConnectId +'_trip', 'started')
            r.zrem('waitingPassengers', passengerConnectId)
        
    except Exception as e:
        print(e)
