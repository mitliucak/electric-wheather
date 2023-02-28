#  将存储在本地 爬取的数据meta信息 入库
import os
import sys
import json
import datetime
import pymysql
import ConfigParam


jar_file = 'mysql-connector-java-8.0.11.jar'
driver = 'com.mysql.cj.jdbc.Driver'

jdbc_url1 = ConfigParam.db_url['jdbc_url1']
jdbc_url2 = ConfigParam.db_url['jdbc_url2']
jdbc_url3 = ConfigParam.db_url['jdbc_url3']
uandp = ConfigParam.db_url['uandp']
