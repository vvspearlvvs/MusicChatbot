import requests
import json
import logging
import time


def top_tracks(artist_id,headers):
    endpoint = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)

    query_params = {'market':'KR'}

    artist_t=requests.get(endpoint,params=query_params,headers=headers)
    artist_tr=json.loads(artist_t.text)

    if artist_t.status_code!=200:
        logging.error(json.loads(artist_t.text))
        if artist_t.status_code == 429: #too much request
            retry_afer = json.loads(artist_t.headers)['retry-After']
            time.sleep(int(retry_afer))
            search_r=requests.get(endpoint,params=query_params,headers=headers)
        elif artist_t.code==401: #get token again
            search_r=requests.get(endpoint,params=query_params,headers=headers)
        else:
            logging.error(json.loads(artist_t.text))

    return artist_tr

