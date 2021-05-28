import requests
import base64
import json
import logging
import time

client_id = ""
client_secret = ""

def get_token(client_id,client_secret):

    endpoint = "https://accounts.spotify.com/api/token"

    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {"Authorization": "Basic {}".format(encoded)}
    payload = {"grant_type": "client_credentials"}

    response = requests.post(endpoint, data=payload, headers=headers)
    #print(json.loads(response.text))
    access_token = json.loads(response.text)['access_token']
    #print(access_token)

    return access_token

def search(artist_name,token):
    endpoint = "https://api.spotify.com/v1/search"

    headers = {"Authorization": "Bearer  {}".format(token)}
    query_params = {'q':artist_name,'type':'artist','limit':'1'}

    search_r=requests.get(endpoint,params=query_params,headers=headers)
    search_ar=json.loads(search_r.text)

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

    return search_ar

def main():
    token = get_token(client_id,client_secret)
    search_data = search('BTS',token)['artists']
    for item in search_data['items']:
        print(item.keys())
main()