import json
import boto3
import requests
import os
from datetime import datetime

# AWS clients
ssm = boto3.client('ssm', region_name='ap-south-1')
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

def get_secret(name):
    """Get secret from Parameter Store"""
    response = ssm.get_parameter(
        Name=name,
        WithDecryption=True
    )
    return response['Parameter']['Value']

def lambda_handler(event, context):
    """
    This is the main function Lambda runs.
    It handles Spotify login for users.
    """
    try:
        # Get Spotify credentials from Parameter Store
        # Never hardcoded — always fetched securely!
        client_id = get_secret('/soulqueue/spotify_client_id')
        client_secret = get_secret('/soulqueue/spotify_client_secret')
        
        # Get the authorization code Spotify sends us
        code = event['queryStringParameters']['code']
        
        # Exchange code for access token
        # This is how Spotify OAuth works:
        # 1. User logs in on Spotify
        # 2. Spotify gives us a code
        # 3. We exchange that code for an access token
        # 4. We use that token to fetch their data
        token_response = requests.post(
            'https://accounts.spotify.com/api/token',
            data={
                'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': 'https://localhost:3000/callback',
                'client_id': client_id,
                'client_secret': client_secret
            }
        )
        
        tokens = token_response.json()
        access_token = tokens['access_token']
        refresh_token = tokens['refresh_token']
        
        # Get user profile from Spotify
        profile_response = requests.get(
            'https://api.spotify.com/v1/me',
            headers={'Authorization': f'Bearer {access_token}'}
        )
        
        profile = profile_response.json()
        user_id = profile['id']
        display_name = profile['display_name']
        
        # Save user to DynamoDB
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
        
        # Return success
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
        # If anything goes wrong log it and return error
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
```

---

## 💡 What This Code Does — Simply Explained
```
User clicks "Login with Spotify"
        ↓
Spotify sends us a secret code
        ↓
We fetch our credentials from Parameter Store
(never hardcoded — always secure!)
        ↓
We exchange the code for an access token
(like exchanging a ticket for entry)
        ↓
We use the token to get user's Spotify profile
        ↓
We save the user to DynamoDB
        ↓
User is now logged in! ✅