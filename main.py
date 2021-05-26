import requests
import base64
import json

client_id = ""
client_secret = ""
endpoint = "https://accounts.spotify.com/api/token"

encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

headers = {"Authorization": "Basic {}".format(encoded)}
payload = {"grant_type": "client_credentials"}

response = requests.post(endpoint, data=payload, headers=headers)
access_token = json.loads(response.text)['access_token']

