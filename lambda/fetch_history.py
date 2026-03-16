import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
ssm = boto3.client('ssm', region_name='ap-south-1')

def get_secret(name):
    return ssm.get_parameter(
        Name=name,
        WithDecryption=True
    )['Parameter']['Value']

def http_get(url, headers):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode('utf-8'))

def detect_language(genres):
    genre_str = ' '.join(genres).lower()
    if any(word in genre_str for word in ['tamil', 'kollywood', 'tamil pop']):
        return 'Tamil'
    elif any(word in genre_str for word in ['filmi', 'bollywood', 'hindi', 'desi pop']):
        return 'Hindi'
    elif any(word in genre_str for word in ['telugu', 'tollywood']):
        return 'Telugu'
    elif any(word in genre_str for word in ['malayalam', 'mollywood']):
        return 'Malayalam'
    elif any(word in genre_str for word in ['kannada', 'sandalwood']):
        return 'Kannada'
    elif any(word in genre_str for word in ['punjabi', 'bhangra']):
        return 'Punjabi'
    elif any(word in genre_str for word in ['bengali', 'bangla']):
        return 'Bengali'
    elif any(word in genre_str for word in ['marathi']):
        return 'Marathi'
    elif any(word in genre_str for word in ['gujarati']):
        return 'Gujarati'
    elif any(word in genre_str for word in ['odia', 'odisha']):
        return 'Odia'
    elif any(word in genre_str for word in ['assamese']):
        return 'Assamese'
    elif any(word in genre_str for word in ['bhojpuri']):
        return 'Bhojpuri'
    elif any(word in genre_str for word in ['rajasthani']):
        return 'Rajasthani'
    elif any(word in genre_str for word in ['haryanvi']):
        return 'Haryanvi'
    elif any(word in genre_str for word in ['nepali']):
        return 'Nepali'
    elif any(word in genre_str for word in ['sinhala', 'sri lanka']):
        return 'Sinhala'
    elif any(word in genre_str for word in ['carnatic', 'hindustani', 'classical indian']):
        return 'Indian Classical'
    elif any(word in genre_str for word in ['k-pop', 'korean', 'k-indie', 'k-rap', 'k-r&b']):
        return 'Korean'
    elif any(word in genre_str for word in ['j-pop', 'japanese', 'j-rock', 'j-indie', 'anime']):
        return 'Japanese'
    elif any(word in genre_str for word in ['mandopop', 'chinese', 'c-pop']):
        return 'Chinese'
    elif any(word in genre_str for word in ['cantopop', 'hong kong']):
        return 'Cantonese'
    elif any(word in genre_str for word in ['opm', 'filipino', 'philippine']):
        return 'Filipino'
    elif any(word in genre_str for word in ['thai pop', 'thai', 'luk thung']):
        return 'Thai'
    elif any(word in genre_str for word in ['vietnamese', 'v-pop']):
        return 'Vietnamese'
    elif any(word in genre_str for word in ['indonesian', 'dangdut']):
        return 'Indonesian'
    elif any(word in genre_str for word in ['malaysian', 'malay pop']):
        return 'Malay'
    elif any(word in genre_str for word in ['burmese', 'myanmar']):
        return 'Burmese'
    elif any(word in genre_str for word in ['arabic', 'khaleeji', 'levant']):
        return 'Arabic'
    elif any(word in genre_str for word in ['turkish', 'arabesk']):
        return 'Turkish'
    elif any(word in genre_str for word in ['persian', 'iranian']):
        return 'Persian'
    elif any(word in genre_str for word in ['hebrew', 'israeli']):
        return 'Hebrew'
    elif any(word in genre_str for word in ['kurdish']):
        return 'Kurdish'
    elif any(word in genre_str for word in ['afrobeats', 'afropop', 'nigerian']):
        return 'Nigerian'
    elif any(word in genre_str for word in ['amharic', 'ethiopian']):
        return 'Amharic'
    elif any(word in genre_str for word in ['swahili', 'bongo flava']):
        return 'Swahili'
    elif any(word in genre_str for word in ['afrikaans']):
        return 'Afrikaans'
    elif any(word in genre_str for word in ['zulu', 'south african']):
        return 'Zulu'
    elif any(word in genre_str for word in ['highlife', 'ghanaian']):
        return 'Ghanaian'
    elif any(word in genre_str for word in ['spanish', 'latin', 'reggaeton', 'flamenco']):
        return 'Spanish'
    elif any(word in genre_str for word in ['french', 'chanson', 'french pop']):
        return 'French'
    elif any(word in genre_str for word in ['samba', 'bossa nova', 'mpb', 'funk carioca']):
        return 'Brazilian Portuguese'
    elif any(word in genre_str for word in ['portuguese', 'fado']):
        return 'Portuguese'
    elif any(word in genre_str for word in ['german', 'schlager', 'deutsch']):
        return 'German'
    elif any(word in genre_str for word in ['italian', 'opera', 'canzone']):
        return 'Italian'
    elif any(word in genre_str for word in ['russian', 'post-soviet']):
        return 'Russian'
    elif any(word in genre_str for word in ['greek']):
        return 'Greek'
    elif any(word in genre_str for word in ['dutch', 'netherlands']):
        return 'Dutch'
    elif any(word in genre_str for word in ['swedish', 'danish', 'norwegian', 'nordic']):
        return 'Nordic'
    elif any(word in genre_str for word in ['polish']):
        return 'Polish'
    elif any(word in genre_str for word in ['ukrainian']):
        return 'Ukrainian'
    elif any(word in genre_str for word in ['romanian']):
        return 'Romanian'
    elif any(word in genre_str for word in ['czech', 'slovak']):
        return 'Czech'
    elif any(word in genre_str for word in ['cumbia', 'vallenato', 'colombian']):
        return 'Colombian Spanish'
    elif any(word in genre_str for word in ['corrido', 'norteno', 'mexican']):
        return 'Mexican Spanish'
    else:
        return 'English'

def get_audio_features(track_id, access_token):
    try:
        features = http_get(
            f'https://api.spotify.com/v1/audio-features/{track_id}',
            {'Authorization': f'Bearer {access_token}'}
        )
        return {
            'valence': features.get('valence', 0.5),
            'energy': features.get('energy', 0.5),
            'tempo': features.get('tempo', 120),
            'danceability': features.get('danceability', 0.5),
            'acousticness': features.get('acousticness', 0.5),
            'instrumentalness': features.get('instrumentalness', 0),
            'speechiness': features.get('speechiness', 0.5),
            'mode': features.get('mode', 1),
            'loudness': features.get('loudness', -10),
            'key': features.get('key', 0)
        }
    except Exception as e:
        print(f"Error getting audio features for {track_id}: {str(e)}")
        return {
            'valence': 0.5,
            'energy': 0.5,
            'tempo': 120,
            'danceability': 0.5,
            'acousticness': 0.5,
            'instrumentalness': 0,
            'speechiness': 0.5,
            'mode': 1,
            'loudness': -10,
            'key': 0
        }

def get_artist_details(artist_id, access_token):
    try:
        artist = http_get(
            f'https://api.spotify.com/v1/artists/{artist_id}',
            {'Authorization': f'Bearer {access_token}'}
        )
        return {
            'genres': artist.get('genres', []),
            'popularity': artist.get('popularity', 0)
        }
    except Exception as e:
        print(f"Error getting artist details: {str(e)}")
        return {'genres': [], 'popularity': 0}

def refresh_access_token(refresh_token, client_id, client_secret):
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

def calculate_mood_scores(audio_features):
    valence = audio_features['valence']
    energy = audio_features['energy']
    acousticness = audio_features['acousticness']
    danceability = audio_features['danceability']
    instrumentalness = audio_features['instrumentalness']
    speechiness = audio_features['speechiness']
    tempo = audio_features['tempo']
    mode = audio_features['mode']

    return {
        'depresso': round((1 - valence) * acousticness * (1 - energy), 3),
        'main_character': round(energy * valence * danceability, 3),
        'locked_in': round((1 - danceability) * (instrumentalness + 0.1) * (1 - speechiness), 3),
        'chaos': round(energy * danceability * (tempo / 200), 3),
        'touch_grass': round(acousticness * (1 - energy) * valence, 3),
        'cupids_bow': round(valence * acousticness * (1 - energy) * mode, 3),
        'eerie': round(instrumentalness * (1 - valence) * acousticness, 3),
        'shutdown': round((1 - energy) * (1 - valence) * (1 - danceability), 3),
        'crying_cute': round((1 - valence) * (1 - energy) * (1 - instrumentalness), 3),
        'slay_mode': round(valence * energy * (1 - acousticness), 3),
        'nostalgia': round(acousticness * valence * (1 - energy), 3),
        'that_girl_era': round(energy * (1 - speechiness) * valence, 3),
        'leave_me_alone': round((1 - valence) * energy * (1 - acousticness), 3),
        'in_my_feels': round((1 - valence) * (1 - instrumentalness) * acousticness, 3)
    }

def lambda_handler(event, context):
    try:
        client_id = get_secret('/soulqueue/spotify_client_id')
        client_secret = get_secret('/soulqueue/spotify_client_secret')

        users_table = dynamodb.Table('soulqueue-users')
        history_table = dynamodb.Table('soulqueue-listening-history')

        users = users_table.scan()['Items']
        processed = 0

        for user in users:
            try:
                user_id = user['user_id']
                refresh_token = user['refresh_token']

                new_tokens = refresh_access_token(
                    refresh_token,
                    client_id,
                    client_secret
                )
                access_token = new_tokens['access_token']

                users_table.update_item(
                    Key={'user_id': user_id},
                    UpdateExpression='SET access_token = :token',
                    ExpressionAttributeValues={':token': access_token}
                )

                recently_played = http_get(
                    'https://api.spotify.com/v1/me/player/recently-played?limit=50',
                    {'Authorization': f'Bearer {access_token}'}
                )

                for item in recently_played.get('items', []):
                    track = item['track']
                    played_at = item['played_at']
                    track_id = track['id']
                    artist_id = track['artists'][0]['id']
                    artist_name = track['artists'][0]['name']

                    audio_features = get_audio_features(
                        track_id,
                        access_token
                    )

                    artist_details = get_artist_details(
                        artist_id,
                        access_token
                    )

                    language = detect_language(
                        artist_details['genres']
                    )

                    mood_scores = calculate_mood_scores(audio_features)

                    history_table.put_item(
                        Item={
                            'user_id': user_id,
                            'played_at': played_at,
                            'track_id': track_id,
                            'track_name': track['name'],
                            'artist_name': artist_name,
                            'artist_id': artist_id,
                            'album_name': track['album']['name'],
                            'duration_ms': str(track['duration_ms']),
                            'language': language,
                            'genres': artist_details['genres'],
                            'artist_popularity': str(artist_details['popularity']),
                            'valence': str(audio_features['valence']),
                            'energy': str(audio_features['energy']),
                            'tempo': str(audio_features['tempo']),
                            'danceability': str(audio_features['danceability']),
                            'acousticness': str(audio_features['acousticness']),
                            'instrumentalness': str(audio_features['instrumentalness']),
                            'speechiness': str(audio_features['speechiness']),
                            'mode': str(audio_features['mode']),
                            'loudness': str(audio_features['loudness']),
                            'key': str(audio_features['key']),
                            'mood_scores': mood_scores,
                            'saved_at': datetime.now().isoformat()
                        }
                    )

                processed += 1
                print(f"Processed {user_id} — {len(recently_played.get('items', []))} tracks")

            except Exception as user_error:
                print(f"Error for user {user.get('user_id')}: {str(user_error)}")
                continue

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully processed {processed} users with full song profiles'
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }