import pymysql
import boto3
import json
import requests
import base64
import logging
from datetime import datetime
import jsonpath
import pandas as pd

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

except:
    logging.error('Database Connect Error')

S3= boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name='ap-northeast-2')
Bucket_name = "musicdatalake"

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

# toptrack 정보를 flat한 형태로 변형하여 S3에 저장
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

    # Spotify API를 통해 top_tracks raw data 수집
    for artist_id,artist_name in artist_result:
        endpoint = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)

        query_params = {'market':'KR'}

        r=requests.get(endpoint,params=query_params,headers=headers)
        raw=json.loads(r.text)

        # top_tracks 데이터는 계층형 구조라 flat한 형태로 변형해서 S3에 저장해야한다.
        # print(raw)
        # raw : "tracks":[
        #      {
        #          "id":"2zlgwqw8BLX2JGB76LIFeF",
        #          "name":"Missing You",
        #          "external_urls":{
        #                "spotify":"https://open.spotify.com/track/2zlgwqw8BLX2JGB76LIFeF"
        #          }
        #          "album":{
        #               "name":"Brother Act.",
        #               "images":[
        #                {
        #                   "height":640,
        #                   "url":"https://i.scdn.co/image/ab67616d0000b27317477a7434c66ac5548b6ab7",
        #                   "width":640
        #                },
        #                {
        #                   "height":300,
        #                   "url":"https://i.scdn.co/image/ab67616d00001e0217477a7434c66ac5548b6ab7",
        #                   "width":300
        #                },
        #                {
        #                   "height":64,
        #                   "url":"https://i.scdn.co/image/ab67616d0000485117477a7434c66ac5548b6ab7",
        #                   "width":64
        #                }
        #                ],  {..생략..}
        #           }, {..생략..}
        #      } ]

        for track in raw['tracks']:
            # 하나의 아티스트에 대한 음악정보 데이터
            top_track_flat = {}
            for k, v in top_track_keys.items(): #k,v : track_id id -> track_name name

                # jsonpath 패키지 이용하여 raw data에서 value path에 해당하는 value를 가져옴
                value = jsonpath.jsonpath(track, v) # EX : ['track id값'] 또는 ['track 이름']
                top_track_flat.update({k: value[0]}) # string으로 받아 아티스트에 대한 음악정보 데이터 생성
                top_track_flat.update({'artist_id': artist_id}) # top_track api 결과에 없던 아티스트 id도 넣어줌

            # flat한 형태로 변형한 결과
            # print(top_track_flat)
            # top_track_flat : {
            #    "track_id":"2zlgwqw8BLX2JGB76LIFeF",
            #    "artist_id":"2hcsKca6hCfFMwwdbFvenJ",
            #    "track_name":"Missing You",
            #    "popularity":62,
            #    "external_url":"https://open.spotify.com/track/2zlgwqw8BLX2JGB76LIFeF",
            #    "album_name":"Brother Act.",
            #    "image_url":"https://i.scdn.co/image/ab67616d00001e0217477a7434c66ac5548b6ab7"
            # }
            top_track_list.append(top_track_flat)

    return top_track_list

# audio 정보를 S3에 저장
def audio_s3(tracks_batch,headers):
    audio_features = []

    for batch in tracks_batch:
        track_id = ','.join(batch) #track id
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(track_id)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)

        # audio_features 데이터는 계층형 구조가 아닌 flat한 구조라 raw data를 그대로 s3에 저장한다.
        # print(raw)
        # raw : {
        #           "danceability":0.641,
        #           "energy":0.807,
        #           "instrumentalness":0,
        #           "loudness":-4.016,
        #           "mode":0,
        #       }

        # instrumentalness 타입통일(float)
        for tmp in raw['audio_features']:
            tmp['instrumentalness']=float(tmp['instrumentalness'])
        audio_features.extend(raw['audio_features']) # append : [[audio데이터, audio데이터]], extend : [audio데이터, audio데이터]

    return audio_features


def main():

    headers = get_header(client_id, client_secret)

    # 1.아티스트의 음악데이터(top_track)
    cursor.execute("Select artist_id,artist_name from artists")
    artist_result = cursor.fetchall()  # ('3HqSLMAZ3g3d5poNaI7GOU', 'IU'), ('3Nrfpe0tUJi4K4DXYWgMUX', 'BTS')

    # flat한 dict형태->dataframe형태->parquet포맷 압축
    top_tracks_dict = toptrack_s3(artist_result,headers)
    top_tracks = pd.DataFrame(top_tracks_dict)
    top_tracks.to_parquet('Test-Batch/top-tracks.parquet', engine='pyarrow', compression='snappy')

    # parquet형태의 top_track 데이터 s3에 저장
    data = open('Test-Batch/top-tracks.parquet', 'rb')
    date_time = datetime.utcnow().strftime('%Y-%m-%d')
    S3.put_object(Body=data,Bucket=Bucket_name,Key='top-tracks/dt={}/top_tracks.parquet'.format(date_time))
    logging.info("top track s3에 저장완료")

    # 2.음악메타데이터(audio_features)
    # 아티스트당 최소 10개의 노래정보가 있어서 track_id를 100개씩 묶어서 처리
    track_ids = [track['track_id'] for track in top_tracks_dict]
    if len(track_ids)>100:
        tracks_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)]
    else:
        tracks_batch = [track_ids] #100개 이하, len(tracks_batch) = 1

    # dict형태->dataframe형태 ->parquet포맷 압축
    audio_features_dict = audio_s3(tracks_batch,headers)
    audio_features = pd.DataFrame(audio_features_dict)
    audio_features.to_parquet('Test-Batch/audio-features.parquet', engine='pyarrow', compression='snappy')

    # parquet형태의 audio 데이터 s3에 저장
    data = open('Test-Batch/audio-features.parquet', 'rb')
    date_time = datetime.utcnow().strftime('%Y-%m-%d')
    S3.put_object(Body=data,Bucket=Bucket_name,Key='audio-features/dt={}/audio_features.parquet'.format(date_time))
    logging.info("audio s3에 저장 ")


if __name__=='__main__':
    main()