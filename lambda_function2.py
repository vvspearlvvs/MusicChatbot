import pymysql
import boto3
import json
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

def related_message(message_item):
    result = {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "carousel": {
                        "type": "listCard",
                        "items": message_item
                    }
                }
            ]
        }
    }
    return result


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
        youtube_url = 'https://www.youtube.com/results?search_query={}'.format(track['artist_name'].replace(' ', '+'))

        # ListCard 형태에 맞게 리턴
        temp_dic = {
            "title": track['track_name'], #타이틀곡명
            "description": track['album']['release_date'], #발매일
            "imageUrl": track['album']['album_image'], #앨범커버이미지
            "link": {
                "web": track['track_url'] #스포티파이링크
            },
            "buttons": [
                {
                    "label": "Youtube에서 검색하기",
                    "action": "webLink",
                    "webLinkUrl": youtube_url
                }
            ]
        }

        items.append(temp_dic)

    return items

def lambda_handler(event):
    print("invoke 된 결과 받은payload")
    print(event)
    mine_id = event['artist_id']
    mine_name = event['artist_name']

    select_query="SELECT other_artist from related_artists where mine_artist ='{}' order by distance desc limit 3".format(mine_id)
    cursor.execute(select_query)
    related_result = cursor.fetchall()
    print(related_result) #(('3HqSLMAZ3g3d5poNaI7GOU',), ('0XATRDCYuuGhk0oE7C0o5G',), ('5TnQc2N1iKlFjYD7CPGvFc',))

    message_item=[]
    for related in related_result:
        other_id =related[0]
        print(other_id)
        related_item=get_toptracks_db(other_id)
        print(related_item)
        message_item.append(related_item)
    print("message")
    message=related_message(message_item)

    return {
        'statusCode':200,
        'body': json.dumps(message),
        'headers': {
            'Access-Control-Allow-Origin': '*',
        }
    }