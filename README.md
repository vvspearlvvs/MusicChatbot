## 음악 챗봇서비스와 AWS 데이터파이프라인
<hr>

전체적인 진행과정은 블로그에서 확인할 수 있습니다: https://pearlluck.tistory.com/notice/572

### 개요
Spotify(음원서비스)API의 아티스트와 음원데이터를 이용하여 <br>
아티스트를 입력하면 유사한 아티스트와 노래를 추천해주는 카카오톡 챗봇 개발

### 프로젝트 기간 
2021.05.30 ~ 20201.06.30

### 사용기술
- Language : Python 3.6 <br>
- DB : AWS RDS(MySQL), AWS DynamoDB, AWS S3, AWS Athena<br>
- Infra : AWS Lambda, API Gateway<br>
- API : Spotify API, Kakao i

### 수행역할 
- Python을 이용하여 Spotify API를 통해 아티스트의 데이터수집
- 데이터 전처리 후 AWS RDS 및 AWS DynamoDB에 데이터저장 
- raw 데이터와 Spotify의 음악메타데이터를 AWS S3에 저장하여 DataLake 구현
- AWS Athena를 통해 유사도를 계산하고, Mysql에 저장하여 DataMart로 활용
- 입력한 아티스트의 정보 뿐만 아니라 유사한 음악을 추천할 수 있는 Serverless 카카오톡 챗봇 서비스 개발

### 아키텍쳐  
AWS API Gateway와 AWS Lambda기반의 Serverless 구조

![Untitled](https://user-images.githubusercontent.com/78723318/123921138-a845e300-d9c1-11eb-8426-ed4e4bc3c019.png)
- 사용자가 챗봇에게 아티스트를 입력하면 API Gateway가 트리거가 되어 lambda 함수가 호출
- 챗봇이 API Gateway를 거쳐 POST 방식으로 보낸 request 메세지를 lambda가 받아 처리한다.   
- 사용자의 Request가 있을때만 동작하여 저렴한 비용으로 서비스 운영가능
- 별도의 서버를 관리할 필요 없이 lambda에 코드작성 및 배포 <br>


### 데이터파이프라인 
### lambda_function.py
Spotify API를 기반으로 데이터를 수집하고, 저장하여 카카오톡 메세지 형태로 응답해주는 로직처리 <br>

![Untitled (1)](https://user-images.githubusercontent.com/78723318/123921481-007ce500-d9c2-11eb-9694-87b4d694c9a5.png)
![Untitled (2)](https://user-images.githubusercontent.com/78723318/123921519-0bd01080-d9c2-11eb-9283-7d0b966fb0db.png)


- 기존 아티스트를 받았을 경우, DynamoDB와 RDS에서 데이터를 조회하여 카카오톡 메세지 형태로 응답
- 신규 아티스트를 받았을 경우, Spotify API를 통해  관련 데이터 수집
- artists 데이터는 AWS RDS, top_track 데이터는 AWS DynamoDB에 저장

### S3_Datalake.py
raw data를 paruqet 포맷으로 압축하여 S3에 저장하는 로직처리 <br>

![Untitled (3)](https://user-images.githubusercontent.com/78723318/123921987-8ac54900-d9c2-11eb-998f-46ce5d1c4a64.png)

- AWS RDS에 저장된 artist_id 로 Spotify API를 통해 raw data 수집
- S3에 저장하기 위해 계층형 구조의 raw data를 flat하게 변형
- paruqet 포맷으로 압축된 데이터를 S3 저장 →  DataLake 구현


### Athena_data.py
Athena 쿼리수행 및 아티스트 간 유사도를 계산하여 RDS에 저장하는 로직처리 <br>

![캡처](https://user-images.githubusercontent.com/78723318/124017992-57b2a200-da22-11eb-8a14-51eca5b16202.PNG)

- S3에 저장된 데이터를 대상으로 AWS Athena 쿼리를 수행하여 구조화된 데이터로 변환 
- 아티스트별 평균 음악메타 데이터를 Euclidean Distance 알고리즘 기반으로 유사도 계산
- 아티스트간의 거리가 가까운 (유사도가 높은) 아티스트를 RDS에 저장 → DataMart 구성


### 프로젝트 결과 
1. 중간 테스트결과 : 입력한 아티스트와 노래정보 전달 <br>

![1차](https://user-images.githubusercontent.com/78723318/123676531-06b67880-d87f-11eb-9735-04a6c206f647.PNG)

2. 최종 테스트결과 : 입력한 아티스트와 가장 유사한 아티스트(Top3)의 노래정보 전달 <br>

![캡처](https://user-images.githubusercontent.com/78723318/123676060-68c2ae00-d87e-11eb-9d5c-4c750a866a41.PNG)


### 보완할점

- S3,Athena 스트립트 처리 자동화를 위한 crontab 또는 airflow 적용
- 유사도 정확성을 높이기 위한 알고리즘 변경적용
- 스포티파이 API 저장된 아티스트 이름으로 검색해야하는 문제점 해결 (예: IU (O), 아이유(X)) <br>
  현재 영어와 한글이 차이가 없는 아티스트명(BTS,IU,BTOB,ITZY,APINK,DAY6 등)으로 테스트 진행
