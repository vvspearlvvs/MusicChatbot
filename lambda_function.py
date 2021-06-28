import json
import requests
import base64
import logging
import time
import pymysql
import boto3
from boto3.dynamodb.conditions import Key,Attr

client_id = ""
client_secret = ""

ACCESS_KEY = ''
SECRET_KEY = ''

rds_host ='localhost' #RDS로 변경시 Public endpoint
rds_user ='root' #RDS로 변경시 admin
rds_pwd = ''
rds_db = 'musicdb'

conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pwd, db=rds_db)
cursor = conn.cursor()

dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name='ap-northeast-2'
    )
table=dynamodb.Table('artist_toptracks') #dynanoDB 파티션키 : track_id

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
def response_select(name,followers,popularity,artist_url,image_url,track_result):
    youtube_url = 'https://www.youtube.com/results?search_query={}'.format(name.replace(' ', '+'))
    result={
        "version": "2.0",
        "template": {
            "outputs": [
                #2.basic 카드형 : DB에 있는 아티스트 입력했을 때 메세지
                {
                    "basicCard": {
                        "title": name,
                        "description": "followers: "+str(followers)+", popularity: "+str(popularity),
                        "thumbnail": {
                            "imageUrl": image_url
                        },
                        "buttons": [
                            {
                                "action":  "webLink",
                                "label": "Spotify에서 검색하기",
                                "webLinkUrl": artist_url
                            }

                        ]
                    }
                },
                #3.list 카드형 : 아티스트의
                {
                    "listCard": {
                        "header": {
                            "title": name+"의 노래를 찾았습니다"
                        },
                        # get_top_tracks는 아티스트의 id를 이용하여 DynamoDB나 API에서 해당 아티스트의 탑 트랙을 찾는 함수
                        # ListCard 형태에 맞게 리턴
                        "items": track_result,
                        "buttons": [
                            {
                                "label": "Youtube에서 검색하기",
                                "action": "webLink",
                                "webLinkUrl": youtube_url
                            }
                        ]
                    }
                },

            ]
        }
    }
    return result

#1.simple text형식 : DB에 없는 아티스트 입력했을 때 메세지
def response_insert():
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

    #RDS작업
    artist_item= search_ar['artists']['items'][0]

    #mysql에 insert row 저장
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
    print("insert mysql 성공")

    return artist_data['artist_id']

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

    print("insert dynamodb 성공")
    print(data)

def get_toptracks_db(id):
    #dynanoDB 파티션키 : track_id
    #track_result=table.query(KeyConditionExpression=Key('artist_id').eq(id))
    select_result = table.scan(FilterExpression = Attr('artist_id').eq(id))
    #print(track_result)

    #최근 발매된 앨범순으로 정렬
    select_result['Items'].sort(key=lambda x: x['album']['release_date'], reverse=True)
    items = []
    #최근 발매된 3개만
    for track in select_result['Items'][:3]:

        # ListCard 형태에 맞게 리턴
        temp_dic = {
            "title": track['track_name'], #타이틀곡명
            "description": track['album']['release_date'], #발매일
            "imageUrl": track['album']['album_image'], #앨범커버이미지
            "link": {
                "web": track['track_url'] #스포티파이링크
            }
        }

        items.append(temp_dic)

    return items




def lambda_handler(event):

    # 메시지 내용은 request의 ['body']에 들어 있음
    request_body = json.loads(event['body'])
    print(request_body)

    #group = request_body['action']['params']['group'] # 그룹아티스트 파라미터
    input_artist = request_body['userRequest']['utterance']

    #1.아티스트검색
    #DB에 있으면,mysql에 검색
    select_query="SELECT * from artists where artist_name ='{}'".format(input_artist)
    cursor.execute(select_query)
    artist_result = cursor.fetchall()

    #db에 있는 아티스트일경우, select결과 리턴 -> top_track 아티스트 가져오기
    if len(artist_result)>0:
        id,name,followers,popularity,artist_url,image_url = artist_result[0]
        track_result=get_toptracks_db(id)

        #2.검색한 아티스트와 유사한 음악추천(음악정보가 Dynamodb에서 가져옴

        select_query="SELECT other_artist from related_artists where mine_artist ='{}' order by distance desc limit 3".format(id)
        cursor.execute(select_query)
        related_result = cursor.fetchall()
        print(related_result) #(('3HqSLMAZ3g3d5poNaI7GOU',), ('0XATRDCYuuGhk0oE7C0o5G',), ('5TnQc2N1iKlFjYD7CPGvFc',), ('4Kxlr1PRlDKEB0ekOCyHgX',), ('7f4ignuCJhLXfZ9giKT7rH',))

        for related in related_result:
            other_id =related[0]
            print(other_id)
            #음악추천결과 메세지 만들기


        #아티스트검색결과 메세지
        message = response_select(name,followers,popularity,artist_url,image_url,track_result)

    #db에 없는 아티스트일경우,'
    else:
        message=response_insert()
        artist_id = get_artist(input_artist,get_header())
        get_toptracks(artist_id,input_artist,get_header())



    print("최종전송 msg")
    print(message)

    return {
        'statusCode':200,
        'body': json.dumps(message),
        'headers': {
            'Access-Control-Allow-Origin': '*',
        }
    }

event={
    "resource":"/music-kakaobot",
    "path":"/music-kakaobot",
    "httpMethod":"POST",
    "headers":{
        "accept":"*/*",
        "Content-Type":"application/json",
        "Host":"mp9dovesu7.execute-api.ap-northeast-2.amazonaws.com",
        "User-Agent":"AHC/2.1",
        "X-Amzn-Trace-Id":"Root=1-60ccd0a3-79b8389864d8d89733cea76a",
        "X-Chappie-Footprint":"chp-9e46d5a9714d40f78c28bd8a5aa90c12",
        "X-Forwarded-For":"219.249.231.40",
        "X-Forwarded-Port":"443",
        "X-Forwarded-Proto":"https",
        "X-Request-Id":"chp-9e46d5a9714d40f78c28bd8a5aa90c12"
    },
    "multiValueHeaders":{
        "accept":[
            "*/*"
        ],
        "Content-Type":[
            "application/json"
        ],
        "Host":[
            "mp9dovesu7.execute-api.ap-northeast-2.amazonaws.com"
        ],
        "User-Agent":[
            "AHC/2.1"
        ],
        "X-Amzn-Trace-Id":[
            "Root=1-60ccd0a3-79b8389864d8d89733cea76a"
        ],
        "X-Chappie-Footprint":[
            "chp-9e46d5a9714d40f78c28bd8a5aa90c12"
        ],
        "X-Forwarded-For":[
            "219.249.231.40"
        ],
        "X-Forwarded-Port":[
            "443"
        ],
        "X-Forwarded-Proto":[
            "https"
        ],
        "X-Request-Id":[
            "chp-9e46d5a9714d40f78c28bd8a5aa90c12"
        ]
    },
    "queryStringParameters":"None",
    "multiValueQueryStringParameters":"None",
    "pathParameters":"None",
    "stageVariables":"None",
    "requestContext":{
        "resourceId":"hmv0y7",
        "resourcePath":"/music-kakaobot",
        "httpMethod":"POST",
        "extendedRequestId":"BIWJiH8KIE0FrQw=",
        "requestTime":"18/Jun/2021:16:58:11 +0000",
        "path":"/default/music-kakaobot",
        "accountId":"129329859727",
        "protocol":"HTTP/1.1",
        "stage":"default",
        "domainPrefix":"mp9dovesu7",
        "requestTimeEpoch":1624035491217,
        "requestId":"21687fa2-b634-4994-a688-a83629cc892d",
        "identity":{
            "cognitoIdentityPoolId":"None",
            "accountId":"None",
            "cognitoIdentityId":"None",
            "caller":"None",
            "sourceIp":"219.249.231.40",
            "principalOrgId":"None",
            "accessKey":"None",
            "cognitoAuthenticationType":"None",
            "cognitoAuthenticationProvider":"None",
            "userArn":"None",
            "userAgent":"AHC/2.1",
            "user":"None"
        },
        "domainName":"mp9dovesu7.execute-api.ap-northeast-2.amazonaws.com",
        "apiId":"mp9dovesu7"
    },
    "body":"{\"bot\":{\"id\":\"60b628c87e223a78e8750a68!\",\"name\":\"스포티파이 검색 봇\"},\"intent\":{\"id\":\"60bd22f6a0293f36984913ef\",\"name\":\"국내아티스트명 블록 \",\"extra\":{\"reason\":{\"code\":1,\"message\":\"OK\"}}},\"action\":{\"id\":\"60bc72c24e460e6c6be02a11\",\"name\":\"API Gateway Server\",\"params\":{\"group\":\"bts\"},\"detailParams\":{\"group\":{\"groupName\":\"\",\"origin\":\"bts\",\"value\":\"bts\"}},\"clientExtra\":{}},\"userRequest\":{\"block\":{\"id\":\"60bd22f6a0293f36984913ef\",\"name\":\"국내아티스트명 블록 \"},\"user\":{\"id\":\"c3e55311e419995dfdbfac37cc496f390887539b442129dc07dbeb3a9d2420ec24\",\"type\":\"botUserKey\",\"properties\":{\"botUserKey\":\"c3e55311e419995dfdbfac37cc496f390887539b442129dc07dbeb3a9d2420ec24\",\"isFriend\":true,\"plusfriendUserKey\":\"cCFcsmWzskCa\",\"bot_user_key\":\"c3e55311e419995dfdbfac37cc496f390887539b442129dc07dbeb3a9d2420ec24\",\"plusfriend_user_key\":\"cCFcsmWzskCa\"}},\"utterance\":\"BTS\",\"params\":{\"surface\":\"Kakaotalk.plusfriend\"},\"lang\":\"ko\",\"timezone\":\"Asia/Seoul\"},\"contexts\":[]}",
    "isBase64Encoded":'false'
}
lambda_handler(event)