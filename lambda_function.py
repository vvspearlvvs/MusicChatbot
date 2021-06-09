import json
import requests
import base64
import logging
import time
import pymysql

client_id = "c87e807943a1483883faaaa881aa43ef"
client_secret = "110de254602e440ea1f72f08f8396ccc"

rds_host ='localhost' #RDS로 변경시 Public endpoint
rds_user ='root' #RDS로 변경시 admin
rds_pwd = 'qwer1234'
rds_db = 'musicdb'

conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pwd, db=rds_db)
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
    select_query="SELECT * from artists where artist_name ='{}'".format(artist_item['name'])
    cursor.execute(select_query)
    db_result = cursor.fetchall()
    print("select결과")
    print(db_result)

    #select결과 있으니까, 처음에 api로 받은 결과 리턴
    if len(db_result)>0:
        #globals()['search_ar']=db_result
        return search_ar

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
    print("insert row 성공")
    conn.commit()
    return search_ar


def lambda_handler(event):

    # 메시지 내용은 request의 ['body']에 들어 있음
    #request_body = json.loads(event['body'])
    #print(request_body)
    #params = request_body['action']['params']
    params = event['action']['params']
    if 'group' in params.keys():
        artist_name = params['group'] # 그룹아티스트 파라미터

        search_result = get_artist(artist_name,get_header())
        print("return결과 ")
        print(search_result)

        result = {
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "당신이 검색한 아티스트는 {}(그룹)입니다.".format(artist_name)
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


#테스트코드
event={
    "intent": {
        "id": "1difwqws70wjl75s5eyanyxx",
        "name": "블록 이름"
    },
    "userRequest": {
        "timezone": "Asia/Seoul",
        "params": {
            "ignoreMe": "true"
        },
        "block": {
            "id": "1difwqws70wjl75s5eyanyxx",
            "name": "블록 이름"
        },
        "utterance": "발화 내용",
        "lang": 'null',
        "user": {
            "id": "908963",
            "type": "accountId",
            "properties": {}
        }
    },
    "bot": {
        "id": "60b628c87e223a78e8750a68",
        "name": "봇 이름"
    },
    "action": {
        "name": "mwmy9qfn9m",
        "clientExtra": 'null',
        "params": {
            "group": "bts"
        },
        "id": "x4j1vafxl0kyq0fx8p1fmpum",
        "detailParams": {
            "group": {
                "origin": "bts",
                "value": "bts",
                "groupName": ""
            }
        }
    }
}
lambda_handler(event)