import json
import requests
import base64
import logging
import time
import pymysql
import boto3

client_id = ""
client_secret = ""

rds_host ='localhost' #RDS로 변경시 Public endpoint
rds_user ='' #RDS로 변경시 admin
rds_pwd = ''
rds_db = 'musicdb'

conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pwd, db=rds_db)
cursor = conn.cursor()

dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id='',
    aws_secret_access_key='',
    region_name='ap-northeast-2'
    )
table=dynamodb.Table('artist_toptracks')

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

    #data의 개수에 맞게 넣어 줌
     placeholders = ', '.join(['%s'] * len(data)) # 형태: '%s, %s, %s, ...'
     columns = ', '.join(data.keys())
     key_placeholders = ', '.join(['{0}=values({0})'.format(k) for k in data.keys()])

     sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)

     #print(sql) # 아래와 같은 형태
     '''
     INSERT INTO artists ( artist_id, artist_name, followers, popularity, artist_url, image_url )
     VALUES ( %s, %s, %s, %s, %s, %s )
     ON DUPLICATE KEY UPDATE artist_id=values(artist_id), artist_name=values(artist_name), followers=values(followers),
     popularity=values(popularity), artist_url=values(artist_url), image_url=values(image_url)
     '''
     cursor.execute(sql, list(data.values()))
#    print(data.values())


#카톡챗봇 메세지 형식 함수
def response_select(artist_name):
    #1.simple text형식 : 메세지만 나갈떄
    result = {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "님이 검색한 아티스트 {} 의 노래입니다".format(artist_name)
                    }
                }
            ]
        }
    }
    return result

def response_insert():
    #1.simple text형식 : 메세지만 나갈떄
    result = {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "오! 새로운 아티스트인걸?! ㄴ저장ㄱ 다시검색 해보세요"
                    }
                }
            ]
        }
    }
    return result

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

    #RDS작업-searchAPI 정상적으로 가져온 경우
    artist_item= search_ar['artists']['items'][0]
    #DB에 있으면,mysql에 검색
    select_query="SELECT artist_id,artist_name,image_url from artists where artist_name ='{}'".format(artist_item['name'])
    cursor.execute(select_query)
    db_result = cursor.fetchall()
    #print("select row 성공")
    #print(db_result)

    #db에 있는거면, select결과 리턴
    if len(db_result)>0:
        #id,name,url=db_result[0]
        return db_result[0]

    #DB에 없으면,mysql에 insert row 저장
    artist_data ={
        'artist_id' : artist_item['id'],
        'artist_name' : artist_item['name'],
        'popularity' : artist_item['popularity'],
        'followers' : artist_item['followers']['total'],
        'artist_url' : artist_item['external_urls']['spotify'],
        'image_url' : artist_item['images'][0]['url'],
        #'genres' : artist_item['genres']
    }
    insert_row(cursor,artist_data,'artists')
    conn.commit()
    #print("insert row 성공")

    #db에 없는거면 () 리턴
    return db_result

def get_toptracks(artist_id,artist_name,headers):
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

    #return artist_tr
    #dynamodb작업-artistAPI 정상적으로 가져온 경우

    for track in artist_tr['tracks']:
        data={
            'artist_id':artist_id,
            'artist_name':artist_name,
            'track_id': track['id'],
            'track_name': track['name'],
            'track_url': track['external_urls']['spotify'],
            'album':
                {'album_id': track['album']['id'],
                 'album_name': track['album']['name'],
                 'album_type': track['album']['album_type'],
                 'album_image': track['album']['images'][0]['url'],
                 'release_date': track['album']['release_date'],
                 'total_tracks': track['album']['total_tracks']
                 }

        }
        #data.update(track)
        table.put_item(Item=data)

    print("dynamdbinsert")


def lambda_handler(event):

    # 메시지 내용은 request의 ['body']에 들어 있음
    #request_body = json.loads(event['body'])
    #print(request_body)
    #params = request_body['action']['params']
    params = event['action']['params']

    #group = params['group'] # 그룹아티스트 파라미터
    artist_name = event['userRequest']['utterance']
    search_result = get_artist(artist_name,get_header())
    #print(search_result)

    #select결과가 있을때
    if search_result:
        id,name,url=search_result
        result = response_select(id)

        get_toptracks(id,name,get_header())
        #dynamodb 결과 가져오기
    #select결과가 없을때
    else:
        result=response_insert()

    print(result)

    return {
        'statusCode':200,
        'body': json.dumps(result),
        'headers': {
            'Access-Control-Allow-Origin': '*',
        }
    }


#테스트코드
event={
    "bot":{
        "id":"60b628c87e223a78e8750a68!",
        "name":"스포티파이 검색 봇"
    },
    "intent":{
        "id":"60bd22f6a0293f36984913ef",
        "name":"국내아티스트명 블록 ",
        "extra":{
            "reason":{
                "code":1,
                "message":"OK"
            }
        }
    },
    "action":{
        "id":"60bc72c24e460e6c6be02a11",
        "name":"API Gateway Server",
        "params":{
            "group":"BTOB"
        },
        "detailParams":{
            "group":{
                "groupName":"",
                "origin":"BTOB",
                "value":"BTOB"
            }
        },
        "clientExtra":{

        }
    },
    "userRequest":{
        "block":{
            "id":"60bd22f6a0293f36984913ef",
            "name":"국내아티스트명 블록 "
        },
        "user":{
            "id":"c3e55311e419995dfdbfac37cc496f390887539b442129dc07dbeb3a9d2420ec24",
            "type":"botUserKey",
            "properties":{
                "botUserKey":"c3e55311e419995dfdbfac37cc496f390887539b442129dc07dbeb3a9d2420ec24",
                "isFriend":'true',
                "plusfriendUserKey":"cCFcsmWzskCa",
                "bot_user_key":"c3e55311e419995dfdbfac37cc496f390887539b442129dc07dbeb3a9d2420ec24",
                "plusfriend_user_key":"cCFcsmWzskCa"
            }
        },
        "utterance":"BTOB",
        "params":{
            "surface":"Kakaotalk.plusfriend"
        },
        "lang":"ko",
        "timezone":"Asia/Seoul"
    },
    "contexts":[

    ]

}
lambda_handler(event)