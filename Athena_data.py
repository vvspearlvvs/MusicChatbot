import boto3
import pymysql
import logging,sys
import time,math
from datetime import datetime

ACCESS_KEY = ''
SECRET_KEY = ''


rds_host ='localhost' #RDS로 변경시 Public endpoint
rds_user ='root' #RDS로 변경시 admin
rds_pwd = ''
rds_db = 'musicdb'

try:
    conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pwd, db=rds_db)
    cursor = conn.cursor()
except:
    logging.error("could not connect to rds")
    sys.exit(1)

Athena= boto3.client('athena',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name='ap-northeast-2')
Buket_name = "musicdatalake"

def query_athena(query,Athena):
    response = Athena.start_query_execution(
        QueryString=query,
        #아테나의 데이터베이스
        QueryExecutionContext={
            'Database': 'musicdatabase'
        },
        ResultConfiguration={
            # 쿼리 결과 저장하는 위치 지정
            'OutputLocation': 's3://' + Buket_name+"/athena-result/",
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )

    return response

def get_query_result(query_id,Athena):

    response = Athena.get_query_execution(
        QueryExecutionId=str(query_id)
    )
    # 쿼리가 완료될 때까지 충분한 시간 대기
    while response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
        if response['QueryExecution']['Status']['State'] == 'FAILED':
            logging.error('QUERY FAILED')
            break
        time.sleep(10) # 데이터 양에 따라 시간소요
        response = Athena.get_query_execution(
            QueryExecutionId=str(query_id)
        )

    response = Athena.get_query_results(
        QueryExecutionId=str(query_id),
        MaxResults=1000 # Athena는 MaxResults가 1000
    )

    return response

def process_data(result):
    data = result['ResultSet']
    columns = [col['VarCharValue'] for col in data['Rows'][0]['Data']]
    print(columns)

    listed_results = []
    for row in data['Rows'][1:]: # 행별로 저장
        values = []
        for col in row['Data']:
            try:
                values.append(col['VarCharValue']) # 각 칼럼의 값들이 {'VarCharValue': value} 형식
            except: # null일 경우?
                values.append('')
        print(values)
        listed_results.append(dict(zip(columns, values)))

    return listed_results


def main():

    #top_track table query
    query = """
        create external table if not exists top_tracks(
        track_id string,
        artist_id string,
        track_name string,
        album_name string,
        popularity int,
        image_url string
        ) partitioned by (dt string)
        stored as parquet location 's3://{}/top-tracks/' 
        tblproperties("parquet.compress" = "snappy")
    """.format(Buket_name)
    r = query_athena(query, Athena)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table top_tracks'
        r = query_athena(query, Athena)

        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            result = get_query_result(r['QueryExecutionId'], Athena)
            #print(result) # 파티션 생성 결과
            print('top_tracks partition update!') # 신규 파티션 생성

    #audio table query
    query = """
        create external table if not exists audio_features(
        duration_ms int,
        key int,
        mode int,
        time_signature int,
        acousticness double,
        danceability double,
        energy double,
        instrumentalness int,
        liveness double,
        loudness double,
        speechiness double,
        valence double,
        tempo double,
        id string
        ) partitioned by (dt string)
        stored as parquet location 's3://{}/audio-features/' 
        tblproperties("parquet.compress" = "snappy")
    """.format(Buket_name)
    r = query_athena(query, Athena)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table audio_features'
        r = query_athena(query, Athena)
        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            result = get_query_result(r['QueryExecutionId'], Athena)
            #print(result)
            print('audio_features partition update!') # 신규 파티션 생성


    #아티스트별 평균수치 계싼
    query = """
        SELECT
            artist_id,
            avg(danceability) as danceability,
            avg(energy) as energy,
            avg(loudness) as loudness,
            avg(speechiness) as speechiness,
            avg(acousticness) as acousticness,
            avg(instrumentalness) as instrumentalness
        FROM
            top_tracks t1
        JOIN
            audio_features t2 on t2.id = t1.track_id
        WHERE
            t1.dt = (select max(dt) from top_tracks)
            and t2.dt = (select max(dt) from audio_features)
        GROUP BY
            t1.artist_id
        """
    r = query_athena(query, Athena)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        result = get_query_result(r['QueryExecutionId'], Athena)
        print('아티스트평균-데이터프로세싱 전 ') #join 결과
        print(result)
        artists=process_data(result)
        print('아티스트평균-데이터프로세싱 후 ') #join 결과
        print(artists)


    # 정규화 위해 수치별 최대, 최소값 계산. 가장 최근 날짜 데이터 사용
    query = """
        SELECT
            MIN(danceability) AS danceability_min,
            MAX(danceability) AS danceability_max,
            MIN(energy) AS energy_min,
            MAX(energy) AS energy_max,
            MIN(loudness) AS loudness_min,
            MAX(loudness) AS loudness_max,
            MIN(speechiness) AS speechiness_min,
            MAX(speechiness) AS speechiness_max,
            ROUND(MIN(acousticness),4) AS acousticness_min,
            MAX(acousticness) AS acousticness_max,
            MIN(instrumentalness) AS instrumentalness_min,
            MAX(instrumentalness) AS instrumentalness_max
        FROM
            audio_features
        WHERE
            dt = (select max(dt) from audio_features)
    """
    r = query_athena(query, Athena)
    result = get_query_result(r['QueryExecutionId'], Athena)
    print('최대최소-데이터프로세싱 전 ') #join 결과
    print(result)
    avgs = process_data(result)[0]
    print('최대최소-데이터프로세싱 후 ') #join 결과
    print(avgs)



if __name__=='__main__':
    main()