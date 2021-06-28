## 음악 챗봇서비스와 AWS 데이터파이프라인
<hr>

전체적인 진행과정은 블로그에서 확인할 수 있습니다:

### 개요
Spotify(음원서비스)API의 아티스트와 음원데이터를 이용하여 
아티스트를 입력하면 유사한 아티스트와 노래를 추천해주는 카카오톡 챗봇 개발

### 프로젝트 기간 
2021.05.30 ~ 20201.06.30

### 사용기술
- Language : Python 3.6 <br>
- DB : AWS RDS(MySQL), AWS DynamoDB, AWS S3 <br>
- Infra : AWS Lambda, API Gateway,AWS Athena<br>
- API : Spotify API, Kakao i

### 수행역할 
- Python을 이용하여 Spotify API를 통해 아티스트의 데이터수집
- 데이터 전처리 후 AWS RDS 및 AWS DynamoDB에 데이터저장 
- raw 데이터와 Spotify의 음악메타데이터를 AWS S3에 저장하여 DataLake 구현
- AWS Athena를 통해 유사도를 계산하고, Mysql에 저장하여 DataMart로 활용
- 입력한 아티스트의 정보 뿐만 아니라 유사한 음악을 추천할 수 있는 Serverless 카카오톡 챗봇 서비스 개발

### 아키텍쳐
1. 사용자가 챗봇에 "아티스트명(예:BTS)"을 입력
2. Spotify API로 아티스트의 정보와 발매일 기준 최근노래를 응답 
![그림1](https://user-images.githubusercontent.com/78723318/123617783-938f1100-d842-11eb-9d43-76eb684e688e.png)

### 데이터파이프라인 
#### lambda_function.py
- mysql에 없는 새로운 아티스트를 챗봇에게 입력했을 경우, Spotify API를 통해 해당정보를 수집
- artists 데이터는 AWS RDS, top_track 데이터는 AWS DynamoDB에 저장
- mysql에 있는 기존의 아티스트를 챗봇에게 입력했을 경우, 저장된 데이터를 전달

#### S3_Datalake.py
- AWS RDS에 저장된 아티스트id로 Spotify API의 음악 메타데이터(danceability,acousticness 등) 수집
- top_track과 audio_features데이터를 paruqet형태로 변형하여 S3에 저장한 DataLake 구성
  ![datalake](https://user-images.githubusercontent.com/78723318/123622785-a0623380-d847-11eb-93a6-43d5e2d2f162.PNG)


#### Athena_data.py
- AWS Athena를 통해 아티스트의 평균 음악메타데이터 쿼리 수행  
- Euclidean Distance 알고리즘 기반으로 아티스트간의 거리가 가까운 (유사도가 높은) 아티스트를 MySQL에 저장한 DataMart 구성
  ![datamark](https://user-images.githubusercontent.com/78723318/123622799-a7894180-d847-11eb-85c4-7690b0feae9c.PNG)

  
CronTab을 통해 배치처리를 자동화하였으며 매일 수행한다. 


### 프로젝트 결과 
1차 테스트결과 : 입력한 아티스트와 노래정보 전달
![image](https://user-images.githubusercontent.com/78723318/123606212-a223fb00-d837-11eb-8125-dadc5a45d481.png)


###
### History
05.25 기획 및 구상 <br>
05.27 카카오채널 생성 및 카카오i 오픈빌더 신청 <br>
05.29-30 Spotify API 탐색-Search API <br>
06.01 카카오i 오픈빌더 승인 -> 시나리오 구성 <br>
06.04-05 Serverless 아키텍쳐(Lambda+API Gateway) 구성 <br>

06.08-09 Artists 데이터저장 및 Lambda 구현 <br>
06.10 로컬환경(mysql) 및 AWS환경(RDS) 테스트완료 <br>

6.15-16 TopTrack 데이터저장 및 Lambda 구현 <br>
6.17 AWS환경(DynamoDB) 테스트완료 및 lambda 배포 <br>
