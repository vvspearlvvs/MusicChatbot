import sys
import logging
import pymysql


rds_host ='' #RDS endpoint
rds_user ='admin'
rds_pwd = ''
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