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

rds_host ='localhost' #RDS: endpoint
rds_user ='root' #RDS: admin
rds_pwd = ''
rds_db = 'musicdb'

try:
    conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pwd, db=rds_db)
    cursor = conn.cursor()

    dynamodb = boto3.resource('dynamodb',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name='ap-northeast-2')
    table=dynamodb.Table('artist_toptracks') #dynanoDB 파티션키 : track_id
except:
    logging.error('could not connect to rds or dynamodb')

# 중간테스트 메세지 : DB에 있는 기존 아티스트 노래정보
# BasicCard타입과 ListCard타입 메세지
def response_select(name,followers,popularity,artist_url,image_url,track_result):
    youtube_url = 'https://www.youtube.com/results?search_query={}'.format(name.replace(' ', '+'))
    result={
        "version": "2.0",
        "template": {
            "outputs": [
                #BasicCard타입 : 아티스트정보 (이름,followers,이미지,Spotify음악URL)
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
                #ListCard타입: 아티스트 노래정보 (노래이름, 발매일, 이미지)
                {
                    "listCard": {
                        "header": {
                            "title": name+"의 노래를 찾았습니다"
                        },
                        #track_result는 아티스트의 id를 이용하여 DynamoDB에서 노래정보를 select한 get_top_track의 리턴값
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

# 중간테스트 메세지 : DB에 없는 새로운 아티스트
# SimpleText타입
def response_insert():
    result = {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "새로운 아티스트를 저장했습니다!"
                    }
                }
            ]
        }
    }
    return result

# 최종테스트 메세지 : 유사한 아티스트의 노래정보
# Carousl타입 메세지
def response_carousl(name,listcard_item):
    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": name+"과 유사한 아티스트의 노래입니다."
                    }
                },
                {
                    "carousel": {
                        "type": "listCard",
                        "items": listcard_item
                    }
                }
            ]
        }
    }

# ListCard타입 메세지
def list_card(track_result,other_name,dist):
    youtube_url = 'https://www.youtube.com/results?search_query={}'.format(other_name.replace(' ', '+'))
    return {
        "listCard": {
            "header": {
                "title": other_name+ "(유사도: "+str(dist)+")"
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
    }


# 신규 아티스트 정보를 mysql에 insert하는 함수
def insert_row(cursor,data,table):

    # sql 쿼리문은 아래와 같은 형태
    '''
    INSERT INTO artists ( artist_id, artist_name, followers, popularity, artist_url, image_url )
    VALUES ( %s, %s, %s, %s, %s, %s )
    ON DUPLICATE KEY UPDATE artist_id=values(artist_id), artist_name=values(artist_name), followers=values(followers),
    popularity=values(popularity), artist_url=values(artist_url), image_url=values(image_url)
    '''

    placeholders = ', '.join(['%s'] * len(data)) # %s, %s, %s, %s, %s, %s
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=values({0})'.format(k) for k in data.keys()])

    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)

    cursor.execute(sql, list(data.values()))

# Spotify API연결을 위한 Token을 가져옴
def get_header():
    endpoint = "https://accounts.spotify.com/api/token"

    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')
    headers = {"Authorization": "Basic {}".format(encoded)}
    payload = {"grant_type": "client_credentials"}
    # POST방식으로 access_token을 요청
    response = requests.post(endpoint, data=payload, headers=headers)
    access_token = json.loads(response.text)['access_token']
    headers = {"Authorization": "Bearer  {}".format(access_token)}

    return headers

# 스포티파이(search API)를 통해 아티스트 정보 수집,변형,RDS 저장
def get_artist_api(artist_name,headers):
    endpoint = "https://api.spotify.com/v1/search"
    query_params = {'q':artist_name,'type':'artist','limit':'1'}

    ## 헤더에 token을 넣은 get방식으로 메세지 요청
    search_r=requests.get(endpoint,params=query_params,headers=headers)
    search_ar=json.loads(search_r.text)

    ## 안정적인 데이터수집을 위한 API호출 예외처리
    if search_r.status_code!=200:
        logging.error(json.loads(search_r.text))

        # too much request일 경우, retry-After 시간(초)만큼 대기 후 재요청
        if search_r.status_code == 429:
            retry_afer = json.loads(search_r.headers)['retry-After']
            time.sleep(int(retry_afer))
            search_r=requests.get(endpoint,params=query_params,headers=headers)

        # API 인증키 만료일 경우, token다시 가져옴
        elif search_r.code==401: #get token again
            search_r=requests.get(endpoint,params=query_params,headers=headers)
        # 이외의 예외상황
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
    }
    insert_row(cursor,artist_data,'artists')
    conn.commit()
    logging.info("신규 아티스트정보 RDS insert완료")

    return artist_data['artist_id']

# 스포티파이(top-tracks API)를 통해 아티스트 음악정보 수집,변형,Dynamodb 저장
def get_toptracks_api(artist_id,artist_name,headers):
    endpoint = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)

    query_params = {'market':'KR'}
    # get방식으로 음악정보 request
    artist_t=requests.get(endpoint,params=query_params,headers=headers)
    artist_tr=json.loads(artist_t.text)

    # spotifyAPI 호출 예외처리 check
    if artist_t.status_code!=200:
        logging.error(json.loads(artist_t.text))

        # too much request일 경우, retry-After 시간(초)만큼 대기 후 재요청
        if artist_t.status_code == 429:
            retry_afer = json.loads(artist_t.headers)['retry-After']
            time.sleep(int(retry_afer))
            search_r=requests.get(endpoint,params=query_params,headers=headers)

        # API 인증키 만료일 경우, token다시 가져옴
        elif artist_t.code==401:
            search_r=requests.get(endpoint,params=query_params,headers=headers)
        # 이외의 예외상황
        else:
            logging.error(json.loads(artist_t.text))

    #Dynamodb작업
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
        table.put_item(Item=data)

    logging.info("신규 아티스트의 음악정보 dynamodb insert완료")

# DynamoDB에서 아티스트 음악정보 쿼리
def get_toptracks_db(id):
    # 파티션키가 아닌 컬럼으로 조회하기 위해 query 대신 scan사용
    select_result = table.scan(FilterExpression = Attr('artist_id').eq(id))

    #최근 발매된 앨범순으로 정렬
    select_result['Items'].sort(key=lambda x: x['album']['release_date'], reverse=True)
    items = []
    for track in select_result['Items'][:3]: #최근 발매된 3개의 데이터만 가져오기
        # ListCard의 item형태에 맞게 변형
        temp_dic = {
            "title": track['track_name'], #타이틀곡명
            "description": track['album']['release_date'], #발매일
            "imageUrl": track['album']['album_image'], #앨범커버이미지
            "link": {
                "web": track['track_url'] #스포티파이링크
            }
        }
        items.append(track['artist_name'])
        items.append(temp_dic)

    return items


def lambda_handler(event,context):

    # 메시지 내용은 request의 ['body']에 들어 있음
    request_body = json.loads(event['body'])
    input_artist = request_body['userRequest']['utterance'] #사용자가 입력한 아티스트명

    # 1.아티스트 DB조회
    select_query="SELECT * from artists where artist_name ='{}'".format(input_artist)
    cursor.execute(select_query)
    artist_result = cursor.fetchall()

    # 1-1.기존 아티스트일 경우 -> 검색한 아티스트의 기본정보 및 음악정보 전달
    if len(artist_result)>0:
        id,name,followers,popularity,artist_url,image_url = artist_result[0]
        track_result=get_toptracks_db(id) # 검색한 아티스트id기준 Dynamodb결과
        logging.info("기존 아티스트의 음악정보 결과")
        logging.info(track_result)
        # 중간 테스트결과. 검색한 아티스트의 기본정보 및 음악정보 메세지 전송
        #message = response_select(name,followers,popularity,artist_url,image_url,track_result)

        # 2.입력받은 아티스트와 유사한 아티스트 음악정보 조회
        select_query="SELECT other_artist,artist_name,distance " \
                     "from related_artists join artists " \
                     "on related_artists.other_artist = artists.artist_id " \
                     "where mine_artist ='{}' " \
                     "order by distance desc " \
                     "limit 3".format(id)
        # 유사한 아티스트 이름을 뽑기 위해 join 수행
        # artist : 입력받은 아티스트정보 테이블, related_artists : 아티스트간 유사도정보 테이블
        cursor.execute(select_query)
        related_result = cursor.fetchall()

        list_card_list=[]
        for related in related_result:
            other_id,other_name,dist = related[0],related[1],related[2] # 추천아티스트id,이름,유사도
            related_track_result = get_toptracks_db(other_id) # 추천 아티스트id 기준 Dynamodb결과
            logging.info("추천 아티스트의 음악정보 결과")
            logging.info(related_track_result)

            list_card_item = list_card(related_track_result,other_name,dist) # 리스트카드 메세지
            list_card_list.append(list_card_item['listCard']) #리스트카드 메세지들의 리스트 케로셀 타입 메세지

        # 최종 테스트결과. 검색한 아티스트와 유사한 아티스의 음악정보 메세지 전송
        message = response_carousl(list_card_list)

    #1-2. 신규 아티스트일 경우
    else:
        message=response_insert()
        artist_id = get_artist_api(input_artist,get_header())
        get_toptracks_api(artist_id,input_artist,get_header())

    logging.info("카카오 챗봇에게 전송하는 최종메세지")
    logging.info(message)

    return {
        'statusCode':200,
        'body': json.dumps(message),
        'headers': {
            'Access-Control-Allow-Origin': '*',
        }
    }

#local환경에서 테스트를 위한 lambda event
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
    "body":"{\"bot\":{\"id\":\"60b628c87e223a78e8750a68!\",\"name\":\"스포티파이 검색 봇\"},\"intent\":{\"id\":\"60bd22f6a0293f36984913ef\",\"name\":\"국내아티스트명 블록 \",\"extra\":{\"reason\":{\"code\":1,\"message\":\"OK\"}}},\"action\":{\"id\":\"60bc72c24e460e6c6be02a11\",\"name\":\"API Gateway Server\",\"params\":{\"group\":\"bts\"},\"detailParams\":{\"group\":{\"groupName\":\"\",\"origin\":\"bts\",\"value\":\"bts\"}},\"clientExtra\":{}},\"userRequest\":{\"block\":{\"id\":\"60bd22f6a0293f36984913ef\",\"name\":\"국내아티스트명 블록 \"},\"user\":{\"id\":\"c3e55311e419995dfdbfac37cc496f390887539b442129dc07dbeb3a9d2420ec24\",\"type\":\"botUserKey\",\"properties\":{\"botUserKey\":\"c3e55311e419995dfdbfac37cc496f390887539b442129dc07dbeb3a9d2420ec24\",\"isFriend\":true,\"plusfriendUserKey\":\"cCFcsmWzskCa\",\"bot_user_key\":\"c3e55311e419995dfdbfac37cc496f390887539b442129dc07dbeb3a9d2420ec24\",\"plusfriend_user_key\":\"cCFcsmWzskCa\"}},\"utterance\":\"bts\",\"params\":{\"surface\":\"Kakaotalk.plusfriend\"},\"lang\":\"ko\",\"timezone\":\"Asia/Seoul\"},\"contexts\":[]}",
    "isBase64Encoded":'false'
}
lambda_handler(event)