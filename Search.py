import requests
import json
import logging
import time


def artist(artist_name,headers):
    endpoint = "https://api.spotify.com/v1/search"

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
            search_r=requests.get(endpoint,params=query_params,headers=headers)
        else:
            logging.error(json.loads(search_r.text))

    return search_ar
