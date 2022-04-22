import json
import redis
import urllib3
import boto3


r = redis.Redis(host= 'redis-12008.c85.us-east-1-2.ec2.cloud.redislabs.com',port= '12008', username= "ausaf", password="Drogon@23")

client = boto3.client('apigatewaymanagementapi', endpoint_url= 'https://cgfkooghv6.execute-api.us-east-1.amazonaws.com/prod')

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
