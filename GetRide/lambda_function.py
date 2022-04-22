import json
import redis
import urllib3
import boto3


r = redis.Redis(host= 'redis-12008.c85.us-east-1-2.ec2.cloud.redislabs.com',port= '12008', username= "ausaf", password="Drogon@23")

client = boto3.client('apigatewaymanagementapi', endpoint_url= 'https://cgfkooghv6.execute-api.us-east-1.amazonaws.com/prod')

def lambda_handler(event, context):
    connectionId = event['requestContext']['connectionId']
    
    getRide(connectionId)
    return {
        'statusCode': 200
    }


def getRide(connectionId):
    try:
        passengerConnectId = r.get(connectionId)
        if passengerConnectId:
            passengerConnectId = passengerConnectId.decode()
            tripStatus = r.get(passengerConnectId +'_trip')
            if tripStatus is None or tripStatus.decode() == 'complete':
                pos = r.geopos("waitingPassengers", passengerConnectId)
                if not pos:
                    client.post_to_connection(ConnectionId=connectionId, Data=json.dumps('No Ride available for you right now!').encode('utf-8'))     
                else:
                    client.post_to_connection(ConnectionId=connectionId, Data=json.dumps('Ride request : Pickup location : ' + ','.join(str(x) for x in pos)).encode('utf-8'))     
        else:
            client.post_to_connection(ConnectionId=connectionId, Data=json.dumps('No Ride available for you right now!').encode('utf-8'))     
    except Exception as e:
        print(e)