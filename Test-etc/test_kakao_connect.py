import json

def lambda_handler(event, context):

    # 메시지 내용은 request의 ['body']에 들어 있음
    request_body = json.loads(event['body'])
    print(request_body)
    params = request_body['action']['params']
    if 'group' in params.keys():
        group = params['group'] # 그룹아티스트 파라미터
        result = {
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "당신이 검색한 아티스트는 {}(그룹)입니다.".format(group)
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