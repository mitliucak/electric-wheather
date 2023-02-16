import requests
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler
import jaydebeapi
import GDFSNcToJson_OEFS_ETC
import GDFSNcToJson_OEFS_RHMI
import GDFSNcToJson_OEFS_RHMX
import GDFSNcToJson_OEFS_RRH
import GDFSNcToJson_OEFS_TMAX
import GDFSNcToJson_OEFS_TMIN
import GDFSNcToJson_OEFS_TMP
import GDFSNcToJson_QPF_PPH
import GDFSNcToJson_QPF_R03
import GDFSNcToJson_QPF_R06
import GDFSNcToJson_QPF_R12
import GDFSNcToJson_QPF_R24
import GDFSNcToJson_SFER_EDA10
import GDFSNcToJson_SHYS_VIS
import LAPSNcToJson
import analysisNc_GDFS
import analysisNc_LAPS

# jar_file = 'sgjdbc_4.3.18.1_20200115.jar'
# driver = 'sgcc.nds.jdbc.driver.NdsDriver'
# jdbc_url1 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqxsync_13306?appname=app_dlqxsync_13306&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# jdbc_url2 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqxsync_13307?appname=app_dlqxsync_13307&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# jdbc_url3 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqx_zl?appname=app_dlqx_zl&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# uandp = ["root", "shanxi_QX"]

jar_file = 'mysql-connector-java-8.0.12.jar'
driver = 'com.mysql.cj.jdbc.Driver'
jdbc_url1 = 'jdbc:mysql://121.52.212.109:13306/06dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
jdbc_url2 = 'jdbc:mysql://121.52.212.109:13307/07dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
jdbc_url3 = 'jdbc:mysql://121.52.212.109:13308/08dlqx_zl?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
uandp = ["root", "123456"]

#获取未解析的nc文件
def getNcFile():
    ncList = []
    try:
        sql = "SELECT jrsj_filename,jrsj_location FROM tb_jrsj WHERE jrsj_jxstorestatus = 0 and jrsj_fileformat = 'nc'"
        try:
            # conn = pymysql.connect(host='localhost', user='root', passwd='', port=3306, db='dlqx13306')  # 连接数据库
            # cursor = conn.cursor()
            conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor = conn.cursor()
            cursor.execute(sql)
            # conn.commit()
            ncList = cursor.fetchall()
            print('1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
            print(ncList)
            print("---1-----------------nclist:----------",ncList)
        except Exception as e:
            print("getNcFile(2) ----- " + str(e))
        finally:
            return ncList
            cursor.close()
            conn.close()
    except Exception as e:
        print("getNcFile(1) ----- " + str(e))

#获取未解析nc文件在三个库里的jrsjid
def getJrsjId(fileName):
    try:
        sql = "SELECT jrsj_id FROM tb_jrsj WHERE jrsj_jxstorestatus = 0 and jrsj_filename = '%s'" % (fileName)
        try:
            # conn1 = pymysql.connect(host='localhost', user='root', passwd='', port=3306, db='dlqx13306')  # 连接数据库
            # cursor1 = conn1.cursor()
            conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor1 = conn1.cursor()
            # conn2 = pymysql.connect(host='localhost', user='root', passwd='', port=3306, db='dlqx13307')  # 连接数据库
            # cursor2 = conn2.cursor()
            conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
            cursor2 = conn2.cursor()
            # conn3 = pymysql.connect(host='localhost', user='root', passwd='', port=3306, db='dlqx13308')  # 连接数据库
            # cursor3 = conn3.cursor()
            conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
            cursor3 = conn3.cursor()
            cursor1.execute(sql)
            # conn1.commit()
            jrsjid1 = cursor1.fetchone()
            cursor2.execute(sql)
            # conn2.commit()
            jrsjid2 = cursor2.fetchone()
            cursor3.execute(sql)
            # conn3.commit()
            jrsjid3 = cursor3.fetchone()

            jrsjids = dict()
            print('3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333')
            print(jrsjid1)
            print(jrsjid2)
            print(jrsjid3)
            jrsjids['one'] = jrsjid1[0]
            jrsjids['two'] = jrsjid2[0]
            jrsjids['three'] = jrsjid3[0]
            print("-2--测试jrsjid3---:",jrsjids['three'])
            print("--3--获取未解析nc文件在三个库中的Jrsjid-------------------------")
        except Exception as e:
            print("----------------getJrsjId(2) ----- " + str(e))
        finally:
            return jrsjids
            cursor1.close()
            conn1.close()
            cursor2.close()
            conn2.close()
            cursor3.close()
            conn3.close()
    except Exception as e:
        print("getJrsjId(1) ----- " + str(e))

def ncDataToMysql(filepath,jrsjid1,jrsjid2,jrsjid3):
    print('44444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444')
    print(filepath)
    print("---5-------filepath----------" + str(filepath))
    if "OEFS_ECT" in filepath:
        GDFSNcToJson_OEFS_ETC.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "OEFS_RHMI" in filepath:
        GDFSNcToJson_OEFS_RHMI.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "OEFS_RHMX" in filepath:
        GDFSNcToJson_OEFS_RHMX.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "OEFS_RRH" in filepath:
        GDFSNcToJson_OEFS_RRH.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "OEFS_TMAX" in filepath:
        GDFSNcToJson_OEFS_TMAX.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "OEFS_TMIN" in filepath:
        GDFSNcToJson_OEFS_TMIN.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "OEFS_TMP" in filepath:
        GDFSNcToJson_OEFS_TMP.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "QPF_PPH" in filepath:
        GDFSNcToJson_QPF_PPH.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "QPF_R03" in filepath:
        GDFSNcToJson_QPF_R03.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "QPF_R06" in filepath:
        GDFSNcToJson_QPF_R06.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "QPF_R12" in filepath:
        GDFSNcToJson_QPF_R12.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "QPF_R24" in filepath:
        GDFSNcToJson_QPF_R24.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "SFER_EDA10" in filepath:
        GDFSNcToJson_SFER_EDA10.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "SHYS_VIS" in filepath:
        GDFSNcToJson_SHYS_VIS.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    elif "LAPS3KM" in filepath:
        LAPSNcToJson.main(filepath,jrsjid1,jrsjid2,jrsjid3)
    else:print("没有进入相应的气象文件路径---------------")
#解析nc文件到mysql
def main():
    ncList = getNcFile()
    for item in ncList:
        fileName = item[0]
        filelocation = item[1]
        print('222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222')
        print(fileName)
        jrsjids = getJrsjId(fileName)
        jrsjid1 = jrsjids['one']
        jrsjid2 = jrsjids['two']
        jrsjid3 = jrsjids['three']
        # 解析nc数据
        print("-4------------进入解析气象数据第一步,根据分类进入不同的方法--------")
        ncDataToMysql(filelocation,jrsjid1,jrsjid2,jrsjid3)
        # 解析nc文件参数
        print("---------------解析完上述文件的文件名filename，进入到下一个：-------------",fileName)
        # if "LAPS3KM" in fileName:
            # 解析3km nc文件参数
            # print("解析3km nc文件参数")
            # analysisNc_LAPS.main(filelocation,jrsjid3,fileName)
        # else:
            # 解析5km nc文件参数
            # print("解析5km nc文件参数")
            # analysisNc_GDFS.main(filelocation,jrsjid3,fileName)
        
        try:
            conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
            cursor1 = conn1.cursor()
            
            conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
            cursor2 = conn2.cursor()
            
            conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
            cursor3 = conn3.cursor()
            
            sql4 = "UPDATE tb_jrsj SET jrsj_jxstorestatus = 1, jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (jrsjid1)
            sql5 = "UPDATE tb_jrsj SET jrsj_jxstorestatus = 1, jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (jrsjid2)
            sql6 = "UPDATE tb_jrsj SET jrsj_jxstorestatus = 1, jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (jrsjid3)


            cursor1.execute(sql4)
            cursor2.execute(sql5)
            cursor3.execute(sql6)
            print(fileName,'修改0为1成功')
        except Exception as e:
            print(fileName,'修改0为1失败',e)
        finally:
            cursor1.close()
            conn1.close()
            cursor2.close()
            conn2.close()
            cursor3.close()
            conn3.close()
        print('以解析完一个nc**********************************************************************************************************************************')


if __name__ == '__main__':
	main()
