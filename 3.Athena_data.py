import boto3
import pymysql
import logging
import time,math

ACCESS_KEY = ''
SECRET_KEY = ''

rds_host ='database-1.cj2sbwq1t1o1.ap-northeast-2.rds.amazonaws.com' #RDS로 변경시 Public endpoint
rds_user ='admin' #RDS로 변경시 admin
rds_pwd = ''
rds_db = 'musicdb'

try:
    conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pwd, db=rds_db)
    cursor = conn.cursor()
except:
    logging.error("Database Connect Error")

Athena= boto3.client('athena',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name='ap-northeast-2')
Bucket_name = "musicdatalake"

# SQL쿼리 실행
def query_athena(query,Athena):

    response = Athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'musicdatabase' # Athena 데이터베이스
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + Bucket_name+"/athena-result/", # 쿼리결과 저장위치
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3' # Amazon S3-managed keys
            }
        }
    )

    return response

# 쿼리 실행결과 리턴
def get_query_result(query_id,Athena):

    response = Athena.get_query_execution(
        QueryExecutionId=str(query_id)
    )

    # 쿼리가 완료될 때까지 충분한 시간 대기
    while response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
        if response['QueryExecution']['Status']['State'] == 'FAILED':
            logging.error('Query 수행 실패')
            break
        time.sleep(10) # 데이터 양에 따라 시간이 걸릴 수 있으니 대기

        response = Athena.get_query_execution(
            QueryExecutionId=str(query_id)
        )

    response = Athena.get_query_results(
        QueryExecutionId=str(query_id),
        MaxResults=1000
    )

    return response

# 데이터 구조화 : 컬럼기반 데이터형식 -> 행기반 데이터형식
def process_data(result):
    data = result['ResultSet']
    columns = [col['VarCharValue'] for col in data['Rows'][0]['Data']] # ex:['artist_id', 'danceability',..]

    listed_results = []
    for row in data['Rows'][1:]:
        values = []
        for col in row['Data']:
            values.append(col['VarCharValue']) # ex:['3Nrfpe0tUJi4K4DXYWgMUX','0.6548'..]
        listed_results.append(dict(zip(columns, values)))

    return listed_results

#1. top_track 테이블생성 쿼리
def query1():
    ## S3 데이터  -> Athena 테이블생성
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
    """.format(Bucket_name)
    r = query_athena(query, Athena) ## S3경로지정시 생성한 날짜(dt) 파티션 정의

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table top_tracks' ## top_tracks 파티션추가 쿼리
        r = query_athena(query, Athena)

        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            result = get_query_result(r['QueryExecutionId'], Athena)
            logging.info('top_tracks partition update!')

#2. audio 테이블생성 쿼리
def query2():
    ## S3 데이터 -> Athena 테이블생성 (날짜기준 파티션)
    query = """
        create external table if not exists audio_features(
        duration_ms int,
        key int,
        mode int,
        time_signature int,
        acousticness double,
        danceability double,
        energy double,
        instrumentalness double,
        liveness double,
        loudness double,
        speechiness double,
        valence double,
        tempo double,
        id string
        ) partitioned by (dt string)
        stored as parquet location 's3://{}/audio-features/' 
        tblproperties("parquet.compress" = "snappy")
    """.format(Bucket_name)
    r = query_athena(query, Athena)  ## S3경로지정시 생성한 날짜(dt) 파티션 정의

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table audio_features' # audio_feature 파티션추가 쿼리
        r = query_athena(query, Athena)
        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            result = get_query_result(r['QueryExecutionId'], Athena)
            logging.info('audio_features partition update!')

#3. 아티스트별 평균 음악메타데이터 조회 쿼리
def query3():

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

    ## 쿼리수행결과(result)
    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        result = get_query_result(r['QueryExecutionId'], Athena)
        # 데이터프로세싱 전 (result) : key, value가 따로따로 저장됨 (row[0]:key1,key2,, row[1:]:values1, values2,,)
        # ex : Row : [
        #      {"Data":[ {"VarCharValue":"artist_id"}, {"VarCharValue":"danceability"}, {..생략..} ] },
        #      {"Data":[ {"VarCharValue":"2KC9Qb60EaY0kW4eH68vr3"}, {"VarCharValue":"0.8013"}, {..생략..} ] },
        #      {"Data":[ {"VarCharValue":"3HqSLMAZ3g3d5poNaI7GOU"}, {"VarCharValue":"0.6491"}, {..생략..} ] },
        #      {..생략..}
        #      ]

        ## 데이터구조화 결과 (artist)
        artists=process_data(result)
        # 데이터프로세싱 후 (artists) : key, value가 쌍으로 저장됨
        # ex : artists :[ {"artist_id":"3Nrfpe0tUJi4K4DXYWgMUX","danceability":"0.6548","..생략.."} ]
        logging.info("Artist의 평균 음악데이터 Processing! ")

        return artists

# 4. 정규화를 위한 음악메타데이터의 최대,최소 조회쿼리
def query4():

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

    ## 쿼리수행결과(result)
    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        result = get_query_result(r['QueryExecutionId'], Athena)
        # 데이터프로세싱 전 (result) : key, value가 따로따로 저장됨
        # ex : Row : [
        #      {"Data":[ {"VarCharValue":"danceability_min"}, {"VarCharValue":"danceability_max"}, {..생략..} ] },
        #      {"Data":[ {"VarCharValue":"0.344"}, {"VarCharValue":"0.874"}, {..생략..} ] },
        #      ]

        ## 데이터구조화 결과 (avgs)
        avgs = process_data(result)[0]
        # 데이터프로세싱 후 (avgs) : key, value가 쌍으로 저장됨
        # ex : artists :[ {"danceability_min":"0.344","danceability_max":"0.874","..생략.."} ]
        logging.info("정규화계산을 위한 min,max 음악데이터 Processing! ")
        return avgs

# min,max를 가지고 정규화 게산
def normalize(x, x_min, x_max):
    normalized = (x-x_min) / (x_max-x_min)
    return normalized

# 유사도 결과를 mysql에 insert하는 함수
def insert_row(cursor, data, table):

    # sql 쿼리문은 아래와 같은 형태
    '''
    INSERT INTO related_artists (mine_artist,other_artist,distance)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE mine_artist=values(mine_artist), other_artist=values(other_artist), distance=values(distance)
    '''

    placeholders = ', '.join(['%s'] * len(data)) # %s, %s, %s
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=values({0})'.format(k) for k in data.keys()])

    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)

    cursor.execute(sql, list(data.values()))


def main():
    start_time = time.time()
    ## 4가지 쿼리수행
    query1() # top_track 테이블 생성 쿼리수행
    query2() # Audio 테이블 생성 쿼리수행
    artists = query3() # 아티스트별 평균 음악메타데이터 쿼리결과 (ex : BTS 노래들의 평균 danceability)
    avgs = query4() # 음악메타데이터의 min,max 값 쿼리결과(ex : 비트,템포같은 danceability의 MIN,MAX)

    metrics = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness']

    ## 유사도 계산 : 유클리드 거리계산 알고리즘 이용
    for i in range(len(artists)):
        related_data = []

        others = artists.copy() # 유사아티스트 후보들
        mine = others.pop(i) # 기준 아티스트

        # mine과 other 사이의 거리가 가까울수록 유사도가 높다.
        for other in others:
            dist = 0
            for m in metrics:

                ## 유사도 : 입력한 아티스트(mine)의 정규화값과 각 아티스트 후보(other)의 정규화값 사이의 거리(dist)
                mine_norm = normalize(float(mine[m]), float(avgs[m + '_min']), float(avgs[m + '_max']))
                other_norm = normalize(float(other[m]), float(avgs[m + '_min']), float(avgs[m + '_max']))
                dist += math.sqrt((mine_norm - other_norm)**2)

            if dist != 0:
                temp = {
                    'mine_artist': mine['artist_id'],
                    'other_artist': other['artist_id'],
                    'distance': dist
                }
                related_data.append(temp)

        ## 유사도 Top3 추출 : 입력한 아티스트와 거리가 가장 가까운(유사도가 높은) 유사아티스트id RDS 저장
        related_data = sorted(related_data, key=lambda x: x['distance'])[:3]
        # ex : [{A,B,0.6],{A,C,0.5},{A.D,0.2},{B,A,0.6},{B,C,0.2},{B,D,0.1}..{..생략..} }
        logging.info("아티스트간 유사도 계산 완료")

        for data in related_data:
            insert_row(cursor, data, 'related_artists') # related_artists : 아티스트간 유사도정보 테이블

    conn.commit()
    cursor.close()
    print("아티스트간 유사도 mysql 저장완료")
    print("실행시간 : {:.1f}s ".format(time.time()-start_time))

if __name__=='__main__':
    main()