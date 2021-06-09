import sys
import logging
import pymysql


rds_host ='database-test.cj2sbwq1t1o1.ap-northeast-2.rds.amazonaws.com'
rds_user ='admin'
rds_pwd = 'qwer1234'
rds_db = 'musicdb'

def lambda_handler():
    conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pwd, db=rds_db)
    curs = conn.cursor()
    print(curs)

    sql = "select * from artists"
    curs.execute(sql)
    rows = curs.fetchall()
    result = 0
    for row in rows:
        result = row[1]

    conn.close()
    return

lambda_handler()