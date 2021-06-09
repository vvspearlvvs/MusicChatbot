import sys
import logging
import pymysql



rds_host ='local' #RDS로 변경시 Public endpoint
rds_user ='root' #RDS로 변경시 admin
rds_pwd = 'qwer1234'
rds_db = 'musicdb'

def lambda_handler():
    conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pwd, db=rds_db)
    curs = conn.cursor()
    print(curs)

    sql = "describe select * from artist"
    curs.execute(sql)
    rows = curs.fetchall()
    result = 0
    for row in rows:
        result = row[1]

    conn.close()
    return

lambda_handler()