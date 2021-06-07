import json
import requests
import base64
import logging
import time
import pymysql

client_id = ""
client_secret = ""

conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
cursor = conn.cursor()

#Spotify API연결을 위한 Token을 가져옴
def get_header():
    endpoint = "https://accounts.spotify.com/api/token"

    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')
    headers = {"Authorization": "Basic {}".format(encoded)}
    payload = {"grant_type": "client_credentials"}
    response = requests.post(endpoint, data=payload, headers=headers)
    access_token = json.loads(response.text)['access_token']

    headers = {"Authorization": "Bearer  {}".format(access_token)}

    return headers

#아티스트 이름으로 검색 : search API 사용
def insert_row(cursor,data,table):
    #mysql insert

def get_artist(artist_name,headers):
    endpoint = "https://api.spotify.com/v1/search"

    query_params = {'q':artist_name,'type':'artist','limit':'1'}

    search_r=requests.get(endpoint,params=query_params,headers=headers)
    search_ar=json.loads(search_r.text)

    #spotifyAPI check
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

    #searchAPI 정상적으로 가져온 경우
    artist_item= search_ar['artists']['items'][0]
    #DB에 있으면,mysql에 검색
    select_query="SELECT * from artists where name ={}".format(artist_item['name'])
    cursor.execute(select_query)
    db_result = cursor.fetcall()

    #select결과 있으니까, 처음에 api로 받은 결과 리턴
    if len(db_result)>0:
        #globals()['search_ar']=db_result
        return search_ar

    #DB에 없으면,mysql에 저장
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
    conn.commit()
    return search_ar

#카카오톡 메세지 타입별 함수

def simple_text(msg):
    return {
        "simpleText": {
            "text": msg
        }
    }

# ListCard 메시지
def list_card(title, imageUrl, items, webLinkUrl):
    return {
        "listCard": {
            "header": {
                "title": title
                # "imageUrl": imageUrl # imageUrl은 제거됨
            },
            "items": items,
            "buttons": [
                {
                    "label": "다른 노래도 보기",
                    "action": "webLink",
                    "webLinkUrl": webLinkUrl
                }
            ]
        }
    }


def lambda_handler(event, context):

    # 메시지 내용은 request의 ['body']에 들어 있음
    request_body = json.loads(event['body'])
    print(request_body)
    params = request_body['action']['params']
    if 'group' in params.keys():
        artist_name = params['group'] # 그룹아티스트 파라미터

        get_artist(artist_name,get_header())

        result = {
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "당신이 검색한 아티스트는 {}(그룹)입니다.".format(group)
                        }
                    }
                ]
            }
        }
    elif 'solo' in params.keys():
        solo=params['solo']
        result = {
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "당신이 검색한 아티스트는 {}(솔로)입니다.".format(solo)
                        }
                    }
                ]
            }
        }

    return {
        'statusCode':200,
        'body': json.dumps(result),
        'headers': {
            'Access-Control-Allow-Origin': '*',
        }
    }