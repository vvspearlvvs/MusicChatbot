import requests
import base64
import json
import logging
import time

client_id = "c87e807943a1483883faaaa881aa43ef"
client_secret = "110de254602e440ea1f72f08f8396ccc"

def get_token(client_id,client_secret):

    endpoint = "https://accounts.spotify.com/api/token"

    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {"Authorization": "Basic {}".format(encoded)}
    payload = {"grant_type": "client_credentials"}

    response = requests.post(endpoint, data=payload, headers=headers)
    print(json.loads(response.text))
    access_token = json.loads(response.text)['access_token']

    return access_token

def search(token):
    endpoint = "https://api.spotify.com/v1/search"

    headers = {"Authorization": "Bearer  {}".format(token)}
    query_params = {'q':'BTS','type':'album','limit':5}

    search_r=requests.get(endpoint,params=query_params,headers=headers)

    if search_r.status_code!=200:
        logging.error(json.loads(search_r.text))
        if search_r.status_code == 429: #too much request
            retry_afer = json.loads(search_r.headers)['retry-After']
            time.sleep(int(retry_afer))
            search_r=requests.get(endpoint,params=query_params,headers=headers)
        elif search_r.code==401: #get token again
            headers = get_token(client_id,client_secret)
            search_r=requests.get(endpoint,params=query_params,headers=headers)
        else:
            logging.error(json.loads(search_r.text))

    return search_r

def main():
    token = get_token(client_id,client_secret)
    search(token)

main()