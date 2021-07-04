import json,os
import requests
import base64
import logging
import time
import pymysql
import boto3
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client_id = os.environ['spotify_id']
client_secret = os.environ['spotify_secret']

rds_host = os.environ['endpoint']
rds_user ='admin'
rds_pwd = 'qwer1234'
rds_db = 'musicdb'

try:
    conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pwd, db=rds_db)
    cursor = conn.cursor()

    dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
    table=dynamodb.Table('artist_toptracks') #dynanoDB 파티션키 : track_id
except:
    logging.error('could not connect to rds or dynamodb')

# 중간테스트 메세지 : DB에 있는 기존 아티스트 노래정보
# BasicCard타입과 ListCard타입 메세지
def kakao_card(name,followers,popularity,artist_url,image_url,track_result):
    youtube_url = 'https://www.youtube.com/results?search_query={}'.format(name.replace(' ', '+'))
    result={
        "version": "2.0",
        "template": {
            "outputs": [
                # 아티스트정보 : 아티스트명, 아티스트이미지, 인기도 등
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
                # 아티스트의 최근노래정보 : 노래이름, 발매날짜, 앨범커버이미지 등
                {
                    "listCard": {
                        "header": {
                            "title": name+"의 최근 노래 입니다"
                        },
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
                # 유사도결과가 없는 안내메세지
                {
                    "simpleText": {
                        "text": " 유사한 아티스트의 노래는 아직 분석중입니다! "
                    }
                },


            ]
        }
    }
    return result

# 중간테스트 메세지 : DB에 없는 새로운 아티스트
# SimpleText타입
def kakao_text(input_artist):
    result = {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "아티스트({})를 저장했습니다!".format(input_artist)
                    }
                }
            ]
        }
    }
    return result

# Carousl타입 메세지
def kakao_carousl(name,listcard_item):
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
    logger.info("API를 통해 artist 데이터수집")
    #print(search_ar)

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

    # RDS작업
    artist_item= search_ar['artists']['items'][0]

    # mysql에 insert row 저장
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
def get_toptracks_api(artist_id,headers):
    endpoint = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)

    query_params = {'market':'KR'}
    ## 헤더에 token을 넣은 get방식으로 메세지 요청
    artist_t=requests.get(endpoint,params=query_params,headers=headers)
    artist_tr=json.loads(artist_t.text)
    logging.info("API를 통해 top-trackss 데이터수집")
    #print(artist_tr)

    ## 안정적인 데이터수집을 위한 API호출 예외처리
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
            'track_id': track['id'],
            'artist_id':artist_id,
            'artist_name':track['artists'][0]['name'],
            'track_name': track['name'],
            'track_url': track['external_urls']['spotify'],
            'album':{
                'album_id': track['album']['id'],
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
    logging.info("Dynamodb에서 top-tracks 조회결과")
    #print(select_result)

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
        items.append(temp_dic)

    return items

def response_message(message):
    return {
        'statusCode':200,
        'body': json.dumps(message),
        'headers': {
            'Access-Control-Allow-Origin': '*',
        }
    }


## 사용자 -> "아티스트입력" -> 챗봇 -> body : "requst msg" -> API Gateway -> "트리거발생" -> Lambda 호출
def lambda_handler(event,context):

    # request 내용은 event의 ['body']에 들어 있음
    request_body = json.loads(event['body'])
    input_artist = request_body['userRequest']['utterance'] #사용자가 입력한 아티스트명

    # 1.아티스트 DB조회
    select_query="SELECT * from artists where artist_name ='{}'".format(input_artist)
    cursor.execute(select_query)
    artist_result = cursor.fetchall()
    logging.info("artists RDS조회 결과")
    #print(artist_result)

    # 1-1.기존 아티스트일 경우 -> RDS와 DynamoDB에 저장된 관련정보 카카오톡 msg전송
    if len(artist_result)>0:
        id,name, followers,popularity,artist_url,image_url = artist_result[0]
        track_result=get_toptracks_db(id) # 아티스트id기준 Dynamodb결과
        logging.info("DynamoDB조회 결과->챗봇 메세지형태로 변환")
        #print(track_result)

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
        logging.info("artist와related_artists조인 결과")
        #print(related_result)

        # s3,athena 스크립트가 수행되어 아티스트에 대한 유사도 분석결과가 있을 경우
        if len(related_result)>0:
            list_card_list=[]
            for related in related_result:
                other_id,other_name,dist = related[0],related[1],related[2] # 추천아티스트id,이름,유사도
                related_track_result = get_toptracks_db(other_id) # 추천 아티스트id 기준 Dynamodb결과
                logging.info("아티스트의 유사도 분석결과 검색완료")
                #print(related_track_result)

                list_card_item = list_card(related_track_result,other_name,dist) # 리스트카드 메세지
                list_card_list.append(list_card_item['listCard']) #리스트카드 메세지들의 리스트 케로셀 타입 메세지

            # 최종메세지.검색한 아티스트와 유사한 아티스의 음악정보 메세지 전송
            logging.info("기존.데이터처리 및 분석 후")
            message = kakao_carousl(name,list_card_list)

        else:
            # 아직 s3,athena 스트립트가 수행되지 않아 유사도 분석결과가 없을 경우
            # 최종메세지.검색한 아티스트정보 메세지 전송
            logging.info("기존.데이터처리 및 분석 전")
            message = kakao_card(name,followers,popularity,artist_url,image_url,track_result)


    #1-2. 신규 아티스트일 경우 -> 스포티파이API를 통해 데이터수집 및 저장
    else:
        logging.info("신규.데이터수집 및 저장")
        artist_id = get_artist_api(input_artist,get_header()) #아티스트정보 RDS 저장
        get_toptracks_api(artist_id,get_header()) #음악정보 DynamoDB 저장
        message=kakao_text(input_artist)

    logging.info("챗봇에게 보낼 최종메세지")
    print(message)
    return {
        'statusCode':200,
        'body': json.dumps(message),
        'headers': {'Access-Control-Allow-Origin': '*',}
    }
