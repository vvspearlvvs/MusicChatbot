import requests
import base64
import json

import Search
import Artist

client_id = ""
client_secret = ""

endpoint = "https://accounts.spotify.com/api/token"

encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')
headers = {"Authorization": "Basic {}".format(encoded)}
payload = {"grant_type": "client_credentials"}
response = requests.post(endpoint, data=payload, headers=headers)
access_token = json.loads(response.text)['access_token']

headers = {"Authorization": "Bearer  {}".format(access_token)}

#1.search artist (with search api)
search_data = Search.artist('BTS', headers)['artists']
for item in search_data['items']:
    #print(item.keys())
    artist_id = item['id']
    artist_name = item['name']
    popularity = item['popularity']
    followers = item['followers']['total']
    artist_url = item['external_urls']['spotify']
    artist_image = item['images'][0]['url']
    genres= item['genres']

#1.1 insert DB(RDS:mysql)

#2.search top track
    artist_track = Artist.top_tracks(artist_id, headers)['tracks']
    for item in artist_track:
        track_id = item['id']
        track_name = item['name']
        ablum = item['ablum'] #{}
        artist = item['artists'] #[]
        track_url = item['external_urls']['spotify']
        track_images =item['images'][0]['url']
    print(artist_track)
#2.2 insert DB(NoSQL:dynamoDB)

