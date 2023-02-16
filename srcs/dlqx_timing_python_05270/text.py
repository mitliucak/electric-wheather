import pymysql
# import mysql.connector

conn1 = pymysql.connect(host='localhost',db='dlqxsync',user='root',password='root',charset='utf8',port=13307)
config = {
    'host': 'localhost',
    'port': 3306,
    'database': '06dlqxsync',
    'user': 'root',
    'password': 'root',
    'charset': 'utf8',
    'use_unicode': True,
    'get_warnings': True,
    'autocommit':True
}
# conn1 = mysql.connector.connect(**config)
cursor = conn1.cursor()
def text():
    time = '10'
    lat = '11'
    lon = '12'
    cloud = '13'
    jrsjid1 = 1
    tup = []
    # arr = (time,lat,lon,cloud,jrsjid1)
    for s in range(21):

        arr = (time, lat, lon, cloud, s)

        # for i in range(0,10):
        tup.append(arr)
        # stus=((1,1,1,1,1),(1,1,1,1,1))
        sql = "INSERT INTO text(time, \
                lat,lon,cloud,jrsjid ) \
               VALUES (%s,%s,%s,%s,%s)"
            # len(tup)

        if len(tup)==10:
            cursor.executemany(sql,tup)
        #     cursor.execute(sql)
            conn1.commit()
            print(tup)
            tup.clear()
            # del (tup)

if __name__=='__main__':
    text()
