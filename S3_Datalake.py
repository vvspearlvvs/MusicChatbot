import pymysql
import boto3
import json
import requests
import base64
import sys, os, logging, time,datetime
import jsonpath
import pandas as pd
#pip install pyarrow

client_id = "c87e807943a1483883faaaa881aa43ef"
client_secret = "110de254602e440ea1f72f08f8396ccc"

ACCESS_KEY = 'AKIAR4HFJBSHRBNI4YGX'
SECRET_KEY = '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'

rds_host ='localhost' #RDS로 변경시 Public endpoint
rds_user ='root' #RDS로 변경시 admin
rds_pw = 'qwer1234'
rds_db = 'musicdb'

S3= boto3.resource('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name='ap-northeast-2')
Buket_name = "Music_Datalake"

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

def main():
    # connect MySQL
    try:
        conn = pymysql.connect(host=rds_host, user=rds_user, passwd=rds_pw, db=rds_db, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    # connect DynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        logging.error('could not connect to dynamodb')
        sys.exit(1)

    headers = get_header(client_id, client_secret)

    cursor.execute("Select artist_id,artist_name from artists")
    artist_result = cursor.fetchall()

    # jsonpath 패키지 이용하여, 원하는 value들만 가져오도록 key path를 설정.
    top_track_keys = {
        "track_id": "id", # track id
        "track_name": "name", #track name
        "followers": "followers",
        "popularity": "popularity",
        "external_url": "external_urls.spotify",
        "album_name": "album.name",
        "image_url": "album.images[1].url"
     }
    #s3에 넣을 flat데이터들 전체
    top_tracks =[]

    for artist_id,artist_name in artist_result:
        endpoint = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)

        query_params = {'market':'KR'}

        artist_t=requests.get(endpoint,params=query_params,headers=headers)
        artist_tr=json.loads(artist_t.text)

        for track in artist_tr['tracks']: # i는 하나의 트랙
            #s3에 넣기 위한 flat한 데이터로 변환
            top_track = {}
            for k, v in top_track_keys.items():
                value = jsonpath.jsonpath(track, v)
                # 해당 위치에 데이터가 없으면 False를 리턴(bool type). 이럴 경우 다음 컬럼으로 넘어감
                if type(value) == bool:
                    continue
                top_track.update({k: value}) # path(v)에 맞게 API에서 찾아 그 위치의 value를 가져옴
                top_track.update({'artist_id': artist_id}) # key 값을 위해 아티스트 id도 넣어줌

            top_tracks.append(top_track)
        print(top_tracks)

    #parquet형태로 저장
    track_ids = [track['track_id'][0] for track in top_tracks] # jsonpath 사용하면 ['id'] 형태로 저장 -> [0]으로 벗겨야 함
    top_tracks = pd.DataFrame(top_tracks)
    top_tracks.to_parquet('top-tracks.parquet', engine='pyarrow', compression='snappy') #top-tracks.parquet 파일 떨굼

    #s3에 저장
    dt = datetime.utcnow().strftime('%Y-%m-%d') # UTC 기준 현재 시간으로. "2020-08-01" 형태
    s3_object = S3.Object('{bucket_name}', 'top-tracks/dt={}/top_tracks.parquet'.format(dt)) # 새로운 폴더(파티션)가 생성이 되는 것
    data = open('top-tracks.parquet', 'rb')
    s3_object.put(Body=data)

if __name__=='__main__':
    main()