import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime

dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

def http_get(url, headers):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode('utf-8'))

def refresh_access_token(refresh_token, client_id, client_secret):
    """Get a new access token using refresh token"""
    data = urllib.parse.urlencode({
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }).encode('utf-8')
    
    req = urllib.request.Request(
        'https://accounts.spotify.com/api/token',
        data=data,
        method='POST'
    )
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode('utf-8'))

def lambda_handler(event, context):
    try:
        # Get all users from DynamoDB
        users_table = dynamodb.Table('soulqueue-users')
        history_table = dynamodb.Table('soulqueue-listening-history')
        
        # Get Parameter Store secrets
        ssm = boto3.client('ssm', region_name='ap-south-1')
        client_id = ssm.get_parameter(
            Name='/soulqueue/spotify_client_id',
            WithDecryption=True
        )['Parameter']['Value']
        client_secret = ssm.get_parameter(
            Name='/soulqueue/spotify_client_secret',
            WithDecryption=True
        )['Parameter']['Value']
        
        # Scan all users
        users = users_table.scan()['Items']
        
        processed = 0
        
        for user in users:
            try:
                user_id = user['user_id']
                refresh_token = user['refresh_token']
                
                # Refresh the access token
                # Spotify access tokens expire after 1 hour
                # Refresh token never expires — we use it to get a new access token
                new_tokens = refresh_access_token(
                    refresh_token,
                    client_id,
                    client_secret
                )
                access_token = new_tokens['access_token']
                
                # Update access token in DynamoDB
                users_table.update_item(
                    Key={'user_id': user_id},
                    UpdateExpression='SET access_token = :token',
                    ExpressionAttributeValues={':token': access_token}
                )
                
                # Fetch recently played tracks from Spotify
                recently_played = http_get(
                    'https://api.spotify.com/v1/me/player/recently-played?limit=50',
                    {'Authorization': f'Bearer {access_token}'}
                )
                
                # Save each track to DynamoDB
                for item in recently_played.get('items', []):
                    track = item['track']
                    played_at = item['played_at']
                    
                    history_table.put_item(
                        Item={
                            'user_id': user_id,
                            'played_at': played_at,
                            'track_id': track['id'],
                            'track_name': track['name'],
                            'artist_name': track['artists'][0]['name'],
                            'album_name': track['album']['name'],
                            'duration_ms': track['duration_ms'],
                            'saved_at': datetime.now().isoformat()
                        }
                    )
                
                processed += 1
                print(f"Processed user {user_id} — {len(recently_played.get('items', []))} tracks saved")
                
            except Exception as user_error:
                print(f"Error processing user {user.get('user_id', 'unknown')}: {str(user_error)}")
                continue
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully processed {processed} users'
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


