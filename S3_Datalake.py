import pymysql
import boto3
import json
import requests
import base64
import logging
from datetime import datetime
import jsonpath
import pandas as pd
#pip install pyarrow

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

S3= boto3.resource('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name='ap-northeast-2')
Buket_name = "musicdatalake"

#Spotify API연결을 위한 Token을 가져옴
def get_header(client_id, client_secret):
    endpoint = "https://accounts.spotify.com/api/token"

    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')
    headers = {"Authorization": "Basic {}".format(encoded)}
    payload = {"grant_type": "client_credentials"}
    response = requests.post(endpoint, data=payload, headers=headers)
    access_token = json.loads(response.text)['access_token']
    headers = {"Authorization": "Bearer  {}".format(access_token)}

    return headers

#toptrack 정보를 S3에 저장
def toptrack_s3(artist_result,headers):
    # jsonpath 패키지 이용하여 계층적인 raw data에서 원하는 value들만 가져오도록 path 지정
    top_track_keys = {
        "track_id": "id",
        "track_name": "name",
        "popularity": "popularity",
        "external_url": "external_urls.spotify",
        "album_name": "album.name",
        "image_url": "album.images[1].url" # track['album']['images'][0]['url']처럼 value값 path의미
    }

    # s3에 넣기 위해 flat하게 변형
    # 전체 아티스트의 음악정보 데이터
    top_track_list =[]

    for artist_id,artist_name in artist_result:
        endpoint = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)

        query_params = {'market':'KR'}

        r=requests.get(endpoint,params=query_params,headers=headers)
        raw=json.loads(r.text)

        # top_tracks 데이터는 계층형 구조라 flat한 형태로 변형해서 S3에 저장해야한다.
        # print(raw)

        for track in raw['tracks']:
            # 하나의 아티스트에 대한 음악정보 데이터
            top_track = {}
            for k, v in top_track_keys.items(): #k,v : track_id id -> track_name name

                # jsonpath 패키지 이용하여 raw data에서 value path에 해당하는 value를 가져옴
                value = jsonpath.jsonpath(track, v) # EX : ['track id값'] 또는 ['track 이름']
                top_track.update({k: value[0]}) # string으로 받아 아티스트에 대한 음악정보 데이터 생성
                top_track.update({'artist_id': artist_id}) # top_track api 결과에 없던 아티스트 id도 넣어줌

            top_track_list.append(top_track) #EX : [{BTS 노래1..}, {BTS 노래2..}, {IU 노래1..}, {IU 노래2..}]

    return top_track_list

# audio 정보를 S3에 저장
def audio_s3(tracks_batch,headers):
    audio_features = []

    for i in tracks_batch:
        ids = ','.join(i) #track id
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text) # audio_features는 flat한 구조라, raw data를 그대로 저장하면 됨.

        # audio_features 데이터는 계층형 구조가 아닌 flat한 구조라 raw data를 그대로 s3에 저장한다.
        # print(raw)

        # instrumentalness 타입통일(float)
        for item in raw['audio_features']:
            item['instrumentalness']=float(item['instrumentalness'])
        audio_features.extend(raw['audio_features']) # append : [[audio데이터, audio데이터]], extend : [audio데이터, audio데이터]

    return audio_features


def main():

    headers = get_header(client_id, client_secret)

    #top_track 데이터
    cursor.execute("Select artist_id,artist_name from artists")
    artist_result = cursor.fetchall()  # ('3HqSLMAZ3g3d5poNaI7GOU', 'IU'), ('3Nrfpe0tUJi4K4DXYWgMUX', 'BTS')

    # dict형태 -> dataframe형태 -> parquet형태
    top_tracks_dict = toptrack_s3(artist_result,headers)
    top_tracks = pd.DataFrame(top_tracks_dict)
    top_tracks.to_parquet('Test-Batch/top-tracks.parquet', engine='pyarrow', compression='snappy') #top-tracks.parquet 파일생성
    track_ids = [track['track_id'] for track in top_tracks_dict]

    # parquet형태의 top_track 데이터 s3에 저장
    date_time = datetime.utcnow().strftime('%Y-%m-%d')
    s3_object = S3.Object(Buket_name, 'top-tracks/dt={}/top_tracks.parquet'.format(date_time)) # dt로 자동 파티션 생성
    data = open('Test-Batch/top-tracks.parquet', 'rb')
    s3_object.put(Body=data)
    logging.info("top track s3에 저장완료")

    #아티스트당 최소 10개의 노래정보가 있어서 100개씩 묶어서 audio 데이터
    tracks_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)]

    # dict형태->dataframe형태 ->parquet형태
    audio_features = audio_s3(tracks_batch,headers)
    audio_features = pd.DataFrame(audio_features)
    audio_features.to_parquet('Test-Batch/audio-features.parquet', engine='pyarrow', compression='snappy')

    #parquet형태의 audio 데이터 s3에 저장
    date_time = datetime.utcnow().strftime('%Y-%m-%d') # UTC 기준 현재 시간으로. "2020-03-23" 형태
    object = S3.Object(Buket_name, 'audio-features/dt={}/audio_features.parquet'.format(date_time)) # 새로운 폴더(파티션)가 생성이 되는 것
    data = open('Test-Batch/audio-features.parquet', 'rb')
    object.put(Body=data)
    logging.info("audio s3에 저장 ")



if __name__=='__main__':
    main()