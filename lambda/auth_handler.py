import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime

ssm = boto3.client('ssm', region_name='ap-south-1')
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

def get_secret(name):
    response = ssm.get_parameter(
        Name=name,
        WithDecryption=True
    )
    return response['Parameter']['Value']

def http_post(url, data):
    encoded_data = urllib.parse.urlencode(data).encode('utf-8')
    req = urllib.request.Request(url, data=encoded_data, method='POST')
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode('utf-8'))

def http_get(url, headers):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode('utf-8'))

def lambda_handler(event, context):
    try:
        client_id = get_secret('/soulqueue/spotify_client_id')
        client_secret = get_secret('/soulqueue/spotify_client_secret')
        
        code = event['queryStringParameters']['code']
        
        tokens = http_post(
            'https://accounts.spotify.com/api/token',
            {
                'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': 'https://localhost:3000/callback',
                'client_id': client_id,
                'client_secret': client_secret
            }
        )
        
        access_token = tokens['access_token']
        refresh_token = tokens['refresh_token']
        
        profile = http_get(
            'https://api.spotify.com/v1/me',
            {'Authorization': f'Bearer {access_token}'}
        )
        
        user_id = profile['id']
        display_name = profile['display_name']
        
        table = dynamodb.Table('soulqueue-users')
        table.put_item(
            Item={
                'user_id': user_id,
                'display_name': display_name,
                'access_token': access_token,
                'refresh_token': refresh_token,
                'created_at': datetime.now().isoformat(),
                'last_login': datetime.now().isoformat()
            }
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': 'Login successful!',
                'user_id': user_id,
                'display_name': display_name
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }