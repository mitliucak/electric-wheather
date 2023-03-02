#  更新jrsj表中错误的文件路径
#  先在08数据库的tb_jrsj进行实验，表中一共1436条数据, 新的路径在/data/history_data下面
import os
import sys
import json
import datetime
from tqdm import tqdm
import pymysql
import ConfigParam
import jaydebeapi
import subprocess


jar_file = 'mysql-connector-java-8.0.11.jar'
driver = 'com.mysql.cj.jdbc.Driver'

jdbc_url1 = ConfigParam.db_url['jdbc_url1']
jdbc_url2 = ConfigParam.db_url['jdbc_url2']
jdbc_url3 = ConfigParam.db_url['jdbc_url3']
uandp = ConfigParam.db_url['uandp']


# 更新爬取的数据在磁盘上面的路径
def update_local_path():
    
    new_root = ConfigParam.urlconfig['localdirpath']
    sql = 'select jrsj_id, jrsj_location from tb_jrsj'
    try:
        conn = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor = conn.cursor()
        cursor.execute(sql)
        jrsj_id_paths = cursor.fetchall()
        new_result = []
        cnt = 0
        for one in tqdm(jrsj_id_paths):
            jrsj_id = one[0]
            filename = one[1].split('/')[-1]
            cmd = 'find ' + new_root + ' -name ' + filename
            result = subprocess.check_output(cmd, shell=True)
            result = result.decode('utf-8').strip()
            size = os.stat(result).st_size
            if size > 10:
                u_sql = "update tb_jrsj set jrsj_location = '" + result + "' where jrsj_id  = '" + str(jrsj_id) + "'"
                cursor.execute(u_sql)
            else:
                u_sql = "delete from tb_jrsj where jrsj_id  = '" + str(jrsj_id) + "'"
                cursor.execute(u_sql)
                cnt += 1
                
    except:
        pass
    finally:
        cursor.close()
        conn.close()
        print("total remove record: {}".format(cnt))

if __name__ == '__main__':
    update_local_path()

