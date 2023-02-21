# 导入所需的 模块与包
import pymysql
import jpype
from dbutils.pooled_db import PooledDB
from dbutils.persistent_db import PersistentDB

# from DBUtils.PooledDB import PooledDB
# from DBUtils.PersistentDB import PersistentDB
import jaydebeapi
import ConfigParam

# jar_file = 'sgjdbc_4.3.18.1_20200115.jar'
# driver = 'sgcc.nds.jdbc.driver.NdsDriver'
# jdbc_url1 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqxsync_13306?appname=app_dlqxsync_13306&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# jdbc_url2 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqxsync_13307?appname=app_dlqxsync_13307&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# jdbc_url3 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqx_zl?appname=app_dlqx_zl&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# conn1 = jaydebeapi.connect(driver, jdbc_url1, ["root", "shanxi_QX"], jar_file)
# conn2 = jaydebeapi.connect(driver, jdbc_url2, ["root", "shanxi_QX"], jar_file)
# conn3 = jaydebeapi.connect(driver, jdbc_url3, ["root", "shanxi_QX"], jar_file)

jar_file = 'mysql-connector-java-8.0.11.jar'
driver = 'com.mysql.cj.jdbc.Driver'
#jdbc_url1 = 'jdbc:mysql://121.52.212.109:13306/06dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
#jdbc_url2 = 'jdbc:mysql://121.52.212.109:13307/07dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
#jdbc_url3 = 'jdbc:mysql://121.52.212.109:13308/08dlqx_zl?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
jdbc_url1 = ConfigParam.db_url['jdbc_url1']
jdbc_url2 = ConfigParam.db_url['jdbc_url2']
jdbc_url3 = ConfigParam.db_url['jdbc_url3']
uandp = ConfigParam.db_url['uandp']
# uandp = ["root", "123456"]
conn1=jaydebeapi.connect(driver,jdbc_url1,['root','123456'],jar_file)
conn2=jaydebeapi.connect(driver,jdbc_url2,['root','123456'],jar_file)
conn3=jaydebeapi.connect(driver,jdbc_url3,['root','123456'],jar_file)
# conn1 = pymysql.connect(host='localhost',db='06dlqxsync',user='root',password='root',charset='utf8')
# conn2 = pymysql.connect(host='localhost',db='07dlqxsync',user='root',password='root',charset='utf8')
# conn3 = pymysql.connect(host='localhost',db='08dlqx_zl',user='root',password='root',charset='utf8')

# # 配置 数据库连接属性
# config1 = {
#     'host': '20.38.167.203',
#     'port': 13306,
#     'database': 'dlqxsync',
#     'user': 'root',
#     'password': 'sx1015',
#     'charset': 'utf8'
# }
#
# # 配置 数据库连接属性
# config2 = {
#     'host': '20.38.167.203',
#     'port': 13307,
#     'database': 'dlqxsync',
#     'user': 'root',
#     'password': 'sx1015',
#     'charset': 'utf8'
# }
#
# # 配置 数据库连接属性
# config3 = {
#     'host': '20.38.167.203',
#     'port': 13308,
#     'database': 'dlqx_zl',
#     'user': 'root',
#     'password': 'sx1015',
#     'charset': 'utf8'
# }
# 配置 数据库连接属性
config1 = {
    'host': 'localhost',
    'port': 13306,
    'database': '06dlqxsync',
    'user': 'root',
    'password': '123456',
    'charset': 'utf8'
}

# 配置 数据库连接属性
config2 = {
    'host': 'localhost',
    'port': 13307,
    'database': '07dlqxsync',
    'user': 'root',
    'password': '123456',
    'charset': 'utf8'
}

# 配置 数据库连接属性
config3 = {
    'host': 'localhost',
    'port': 13308,
    'database': '08dlqx_zl',
    'user': 'root',
    'password': '123456',
    'charset': 'utf8'
}

def get_db_pool(is_mult_thread,configtype):
    '''
    创建数据库连接池
    :param is_mult_thread: 传入Bool值，True：多线程连接  False：单线程连接
    :param configtype 连接哪一个数据库
    :return:
    '''
    # 多线程连接模式
    if configtype == 1:
        config = config1
    elif configtype == 2:
        config = config2
    else:
        config = config3
    if is_mult_thread:
        poolDB = PooledDB(
            # 指定数据库连接驱动
            creator=pymysql,
            # 连接池允许的最大连接数,0和None表示没有限制
            maxconnections=3,
            # 初始化时,连接池至少创建的空闲连接,0表示不创建
            mincached=2,
            # 连接池中空闲的最多连接数,0和None表示没有限制
            maxcached=5,
            # 连接池中最多共享的连接数量,0和None表示全部共享(其实没什么卵用)
            maxshared=3,
            # 连接池中如果没有可用共享连接后,是否阻塞等待,True表示等等,
            # False表示不等待然后报错
            blocking=True,
            # 开始会话前执行的命令列表
            setsession=[],
            # ping Mysql服务器检查服务是否可用
            ping=0,
            **config
        )
    # 单线程连接模式
    else:
        poolDB = PersistentDB(
            # 指定数据库连接驱动
            creator=pymysql,
            # 一个连接最大复用次数,0或者None表示没有限制,默认为0
            # maxusage=1000,
            **config
        )
    return poolDB

def get_db_pool2(configtype):
    if configtype == 1:
        conn = conn1
    elif configtype == 2:
        conn = conn2
    else:
        conn = conn3
    return conn
    
# if __name__ == '__main__':
#     # 以单线程的方式初始化数据库连接池
#     db_pool = get_db_pool(False)
#     # 从数据库连接池中取出一条连接
#     conn = db_pool.connection()
#     cursor = conn.cursor()
#     # 测试 查询
#     cursor.execute('select * from xxx')
#     # 取出一条查询结果
#     result = cursor.fetchone()
#     print(result)
#     # 把连接返还给连接池
#     conn.close()


