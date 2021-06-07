import requests
import json
import logging
import time
import pymysql
mysql_host = 'localhost'
mysql_user = 'root'
mysql_pwd = ''
mysql_db = 'mydb'


mysql_client = pymysql.connect(user=mysql_user,passwd=mysql_pwd,host=mysql_host,db=mysql_db,autocommit=True)
cursor = mysql_client.cursor(pymysql.cursors.Cursor)

def artist(artist_name,headers):
    endpoint = "https://api.spotify.com/v1/search"

    query_params = {'q':artist_name,'type':'artist','limit':'1'}

    search_r=requests.get(endpoint,params=query_params,headers=headers)
    search_ar=json.loads(search_r.text)

    #spotify check
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

    #spotify api result
    artist_item= search_ar['artists']['items'][0]

    select_query="SELECT * from artists where name ={}".format(artist_item['name'])
    cursor.execute(select_query)
    db_result = cursor.fetcall()

    artist_data ={
        'artist_id' : artist_item['id'],
        'artist_name' : artist_item['name'],
        'popularity' : artist_item['popularity'],
        'followers' : artist_item['followers']['total'],
        'artist_url' : artist_item['external_urls']['spotify'],
        'artist_image' : artist_item['images'][0]['url'],
        'genres' : artist_item['genres']
    }
    insert_row(cursor,artist_data,'artists')

    return search_ar
