import requests
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import pymysql
import DbutilsPool
import time
import os
import zipfile
from bs4 import BeautifulSoup
import shutil
import jaydebeapi

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

#将大区里面的tb_qxsj里面的接口数据 做一定的处理存储进 tb_jrsj



#逐小时逐日站点TXT可以用--从大区的tb_qxsj处理之后写入tb_jrsj：
def QxsjToJrsjZd(qxsjs):
    try:
        for item in qxsjs:
            # print(item)
            jrsjfiletimestr = item[2].rsplit("_", 1)[-1].split(".")[0]
            jrsjfiletime = datetime.datetime.strptime(jrsjfiletimestr, '%Y%m%d%H%M%S')

            if "_GJ_" in item[2]:
                jrsjstationcategory = "国家站"
            elif "_ZD_" in item[2]:
                jrsjstationcategory = "自动站"

            jrsjfilename = item[2]
            jrsjfileformat = item[2].split(".")[-1]
            jrsjfilesize = item[4]
            jrsjjrinterface = item[1]
            jrsjjrtime = datetime.datetime.strptime(item[5], '%Y-%m-%dT%H:%M:%SZ')
            jrsjlocation = item[6].replace("\\", "\\\\")
            jrsjqxtype = item[3]

            sql = "INSERT INTO tb_jrsj(jrsj_filetime, jrsj_stationcategory, jrsj_filename, jrsj_fileformat, jrsj_filesize, jrsj_jrinterface, jrsj_jrtime, jrsj_location,jrsj_qxtype) \
                       VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
            jrsjfiletime, jrsjstationcategory, jrsjfilename, jrsjfileformat, jrsjfilesize, jrsjjrinterface, jrsjjrtime,
            jrsjlocation, jrsjqxtype)
            # print(sql)
            try:
                conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
                cursor1 = conn1.cursor()
                cursor1.execute(sql)
                print('tb_jrsj表存入成功1111111111111',jrsjfilename)
                # conn1.commit()
            except Exception as e:
                print("QxsjToJrsjZd(1) ----- ")
            finally:
                cursor1.close()
                conn1.close()

            try:
                conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
                cursor2 = conn2.cursor()
                cursor2.execute(sql)
                print('tb_jrsj表存入成功222222222222',jrsjfilename)
                # conn2.commit()
            except Exception as e:
                print("QxsjToJrsjZd(2) ----- ",e)
            finally:
                cursor2.close()
                conn2.close()

            try:
                conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
                cursor3 = conn3.cursor()
                cursor3.execute(sql)
                print('tb_jrsj表存入成功333333333333333',jrsjfilename)
                # conn3.commit()
            except Exception as e:
                print("QxsjToJrsjZd(3) ----- ")
            finally:
                cursor3.close()
                conn3.close()

    except Exception as e:
        print("QxsjToJrsjZd ----- ")


def AlarmTypeCodeToAlarmType(AlarmTypeCode):
    if AlarmTypeCode == "11B01":
        return "台风"
    elif AlarmTypeCode == "11B03":
        return "暴雨"
    elif AlarmTypeCode == "11B04":
        return "暴雪"
    elif AlarmTypeCode == "11B05":
        return "寒潮"
    elif AlarmTypeCode == "11B05":
        return "寒潮"
    elif AlarmTypeCode == "11B06":
        return "大风"
    elif AlarmTypeCode == "11B07":
        return "沙尘暴"
    elif AlarmTypeCode == "11B09":
        return "高温"
    elif AlarmTypeCode == "11B14":
        return "雷电"
    elif AlarmTypeCode == "11B15":
        return "冰雹"
    elif AlarmTypeCode == "11B16":
        return "霜冻"
    elif AlarmTypeCode == "11B17":
        return "大雾"
    elif AlarmTypeCode == "11B19":
        return "霾"
    elif AlarmTypeCode == "11B20":
        return "雷雨大风"
    elif AlarmTypeCode == "11B21":
        return "道路结冰"
    elif AlarmTypeCode == "11B22":
        return "干旱"
    elif AlarmTypeCode == "11B25":
        return "森林火灾"
    else:
        return AlarmTypeCode


# 灾害预警--从大区的tb_qxsj处理之后写入tb_jrsj：
def QxsjToJrsjAlarm(qxsjs):
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()

        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()

        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        for item in qxsjs:
            jrsjfiletimestr = item[2].split("_")[1]
            jrsjfiletime = datetime.datetime.strptime(jrsjfiletimestr, '%Y%m%d%H%M%S')
            alarmtypecode = item[2].split("_")[2]
            jrsjalarmtype = AlarmTypeCodeToAlarmType(alarmtypecode)
            jrsjfilename = item[2]
            jrsjfileformat = item[2].split(".")[-1]
            jrsjfilesize = item[4]
            jrsjjrinterface = item[1]
            jrsjjrtime = datetime.datetime.strptime(item[5], '%Y-%m-%dT%H:%M:%SZ')
            jrsjlocation = item[6].replace("\\", "\\\\")
            jrsjqxtype = item[3]

            sql = "INSERT INTO tb_jrsj(jrsj_filetime, jrsj_alarmtype, jrsj_filename, jrsj_fileformat, jrsj_filesize, jrsj_jrinterface, jrsj_jrtime, jrsj_location,jrsj_qxtype) \
                       VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s')" % (
            jrsjfiletime, jrsjalarmtype, jrsjfilename, jrsjfileformat, jrsjfilesize, jrsjjrinterface, jrsjjrtime,
            jrsjlocation, jrsjqxtype)
            try:
                cursor1.execute(sql)
                # conn1.commit()
            except Exception as e:
                print("QxsjToJrsjAlarm(1) ----- ")

            try:
                cursor2.execute(sql)
                # conn2.commit()
            except Exception as e:
                print("QxsjToJrsjAlarm(2) ----- ")

            try:
                cursor3.execute(sql)
                # conn3.commit()
            except Exception as e:
                print("QxsjToJrsjAlarm(3) ----- ")

    except Exception as e:
        print("QxsjToJrsjAlarm ----- ")
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


# 3KM网格--从大区的tb_qxsj处理之后写入tb_jrsj：
def QxsjToJrsj3KMwg(qxsjs):
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()

        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()

        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        for item in qxsjs:
            jrsjfiletimestr = item[2].split("_")[-2]
            jrsjfiletime = datetime.datetime.strptime(jrsjfiletimestr, '%Y%m%d%H%M')
            jrsjfilename = item[2]
            jrsjfileformat = item[2].split(".")[-1]
            jrsjfilesize = item[4]
            jrsjjrinterface = item[1]
            jrsjjrtime = datetime.datetime.strptime(item[5], '%Y-%m-%dT%H:%M:%SZ')
            jrsjlocation = item[6].replace("\\", "\\\\")
            jrsjqxtype = item[3]

            sql = "INSERT INTO tb_jrsj(jrsj_filetime, jrsj_filename, jrsj_fileformat, jrsj_filesize, jrsj_jrinterface, jrsj_jrtime, jrsj_location,jrsj_qxtype) \
                       VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (
            jrsjfiletime,jrsjfilename,jrsjfileformat,jrsjfilesize,jrsjjrinterface,jrsjjrtime,jrsjlocation,
            jrsjqxtype)
            try:
                cursor1.execute(sql)
                print('1111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                print('tb_jrsj存入成功',jrsjfilename)
                # conn1.commit()
            except Exception as e:
                print("QxsjToJrsj3KMwg(1) ----- ")

            try:
                cursor2.execute(sql)
                # conn2.commit()
            except Exception as e:
                print("QxsjToJrsj3KMwg(2) ----- ")

            try:
                cursor3.execute(sql)
                # conn3.commit()
            except Exception as e:
                print("QxsjToJrsj3KMwg(3) ----- ")

    except Exception as e:
        print("QxsjToJrsj3KMwg ----- ")
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


def YbNameToYbfactor(filename):
    if "_TMP_" in filename:
        return "温度"
    elif "_TMAX_" in filename:
        return "最高温度"
    elif "_TMIN_" in filename:
        return "最低温度"
    elif "_RRH_" in filename:
        return "湿度"
    elif "_RHMX_" in filename:
        return "最大相对湿度"
    elif "_RHMI_" in filename:
        return "最小相对湿度"
    elif "_QPF_R24_" in filename:
        return "24小时降水"
    elif "_QPF_R12_" in filename:
        return "12小时降水"
    elif "_QPF_R06_" in filename:
        return "6小时降水"
    elif "_QPF_R03_" in filename:
        return "3小时降水"
    elif "_QPF_PPH_" in filename:
        return "降水相态"
    elif "_EDA10_" in filename:
        return "风向风速（逐3小时）"
    elif "_OEFS_ECT_" in filename:
        return "云量"
    elif "_SHYS_VIS_" in filename:
        return "能见度"
    else:
        return ""


# 5KM预报网格--从大区的tb_qxsj处理之后写入tb_jrsj：
def QxsjToJrsj5KMwg(qxsjs):
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()

        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()

        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        for item in qxsjs:
            if item[2] != 'ShanXi_.nc':
                jrsjfiletimestr = item[2].split("_")[-1].split(".")[0]
                jrsjfiletime = datetime.datetime.strptime(jrsjfiletimestr[:-5], '%Y%m%d%H%M%S')
                jrsjybfactor = YbNameToYbfactor(item[2])
                jrsjfilename = item[2]
                jrsjfileformat = item[2].split(".")[-1]
                jrsjfilesize = item[4]
                jrsjjrinterface = item[1]
                jrsjjrtime = datetime.datetime.strptime(item[5], '%Y-%m-%dT%H:%M:%SZ')
                jrsjlocation = item[6].replace("\\", "\\\\")
                jrsjqxtype = item[3]

                # print(jrsjfiletime)

                sql = "INSERT INTO tb_jrsj(jrsj_filetime, jrsj_ybfactor, jrsj_filename, jrsj_fileformat, jrsj_filesize, jrsj_jrinterface, jrsj_jrtime, jrsj_location,jrsj_qxtype) \
                           VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                jrsjfiletime,jrsjybfactor,jrsjfilename,jrsjfileformat,jrsjfilesize,jrsjjrinterface,jrsjjrtime,
                jrsjlocation,jrsjqxtype)
                try:
                    cursor1.execute(sql)
                    print('1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                    print('tb_jrsj存入成功111111111',jrsjfilename)
                    # conn1.commit()
                except Exception as e:
                    print("QxsjToJrsj5KMwg(1) ----- ",e)

                try:
                    cursor2.execute(sql)
                    print('1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                    print('tb_jrsj存入成功2222222222',jrsjfilename)
                    # conn2.commit()
                except Exception as e:
                    print("QxsjToJrsj5KMwg(2) ----- ",e)

                try:
                    cursor3.execute(sql)
                    print('1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                    print('tb_jrsj存入成功3333333333',jrsjfilename)
                    # conn3.commit()
                except Exception as e:
                    print("QxsjToJrsj5KMwg(3) ----- ",e)

    except Exception as e:
        print("QxsjToJrsj5KMwg ----- ",e)
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


# 通用：如辐射逐小时接口--从大区的tb_qxsj处理之后写入tb_jrsj：
def QxsjToJrsjCommon(qxsjs):
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()

        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()

        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        for item in qxsjs:
            jrsjfiletimestr = item[2].rsplit("_", 1)[-1].split(".")[0]
            jrsjfiletime = datetime.datetime.strptime(jrsjfiletimestr, '%Y%m%d%H%M%S')
            jrsjfilename = item[2]
            jrsjfileformat = item[2].split(".", 1)[-1]
            jrsjfilesize = item[4]
            jrsjjrinterface = item[1]
            jrsjjrtime = datetime.datetime.strptime(item[5], '%Y-%m-%dT%H:%M:%SZ')
            jrsjlocation = item[6].replace("\\", "\\\\")
            jrsjqxtype = item[3]

            sql = "INSERT INTO tb_jrsj(jrsj_filetime,jrsj_filename,jrsj_fileformat,jrsj_filesize,jrsj_jrinterface,jrsj_jrtime,jrsj_location,jrsj_qxtype) \
                       VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (
            jrsjfiletime,jrsjfilename,jrsjfileformat,jrsjfilesize,jrsjjrinterface,jrsjjrtime,jrsjlocation,
            jrsjqxtype)
            # print(sql)
            try:
                cursor1.execute(sql)
                print('111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                print('tb_jrsj存入成功',jrsjfilename)
                # conn1.commit()
            except Exception as e:
                print("QxsjToJrsjCommon(1) ----- ")
                print(e)

            try:
                cursor2.execute(sql)
                # conn2.commit()
            except Exception as e:
                print("QxsjToJrsjCommon(2) ----- ")
                print(e)

            try:
                cursor3.execute(sql)
                # conn3.commit()
            except Exception as e:
                print("QxsjToJrsjCommon(3) ----- ")
                print(e)

    except Exception as e:
        print("QxsjToJrsjCommon ----- ")
        print(e)
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


# 通用：OCF--从大区的tb_qxsj处理之后写入tb_jrsj：
def QxsjToJrsjOCF(qxsjs):
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()

        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()

        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        for item in qxsjs:
            jrsjfiletimestr = item[2].rsplit("_")[-2]
            jrsjfiletime = datetime.datetime.strptime(jrsjfiletimestr, '%Y%m%d%H%M%S')
            jrsjfilename = item[2]
            jrsjfileformat = item[2].split(".", 1)[-1]
            jrsjfilesize = item[4]
            jrsjjrinterface = item[1]
            jrsjjrtime = datetime.datetime.strptime(item[5], '%Y-%m-%dT%H:%M:%SZ')
            jrsjlocation = item[6].replace("\\", "\\\\")
            jrsjqxtype = item[3]

            sql = "INSERT INTO tb_jrsj(jrsj_filetime, jrsj_filename, jrsj_fileformat, jrsj_filesize, jrsj_jrinterface, jrsj_jrtime, jrsj_location,jrsj_qxtype) \
                       VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (
            jrsjfiletime,jrsjfilename,jrsjfileformat,jrsjfilesize,jrsjjrinterface,jrsjjrtime,jrsjlocation,
            jrsjqxtype)
            # print(sql)
            try:
                cursor1.execute(sql)
                print('111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                print('tb_jrsj存入成功',jrsjfilename)
                # conn1.commit()
            except Exception as e:
                print("QxsjToJrsjOCF(1) ----- ",e)

            try:
                cursor2.execute(sql)
                # conn2.commit()
            except Exception as e:
                print("QxsjToJrsjOCF(2) ----- ",e)

            try:
                cursor3.execute(sql)
                # conn3.commit()
            except Exception as e:
                print("QxsjToJrsjOCF(3) ----- ",e)

    except Exception as e:
        print("QxsjToJrsjOCF ----- ")
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


# qxsj_jrsjstrorestatus：标志tb_jrsj某一项是否已经同步到tb_jrsj中，默认为0,0标志未同步，1标志该项已经同步
def GetQxsjByTypeNoTb(qxsjtype):
    qxsj = []
    try:
        sql = "SELECT * FROM tb_qxsj WHERE qxsj_jrsjstrorestatus = 0 and qxsj_type = '%s'" % (qxsjtype)
        try:
            conn = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
            cursor = conn.cursor()
            cursor.execute(sql)
            # conn.commit()
            qxsj = cursor.fetchall()
        except Exception as e:
            print("GetQxsjByTypeNoTb(2) ----- ")
        finally:
            return qxsj
            cursor.close()
            conn.close()
    except Exception as e:
        print("GetQxsjByTypeNoTb(1) ----- ")


# 数据存储成功之后使用：将qxsj_jrsjstrorestatus标志未改为1
def SetQxsjJrsjStatus1(qxsjs):
    try:
        conn = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor = conn.cursor()

        for item in qxsjs:
            sql = "UPDATE  tb_qxsj SET qxsj_jrsjstrorestatus = 1 WHERE id = %s " % (item[0])
            try:
                cursor.execute(sql)
                # conn.commit()
            except Exception as e:
                print("SetQxsjJrsjStatus1(2) ----- ")
                print(e)
    except Exception as e:
        print("SetQxsjJrsjStatus1(1) ----- ")
        print(e)
    finally:
        cursor.close()
        conn.close()

# if __name__ == '__main__':
#     qxsjsGjxs = GetQxsjByTypeNoTb("国家站小时")
#     QxsjToJrsjZd(qxsjsGjxs)
#     qxsjsZdxs = GetQxsjByTypeNoTb("自动站小时")
#     QxsjToJrsjZd(qxsjsZdxs)
#     qxsjsWg3km = GetQxsjByTypeNoTb("3KM实时网格")
#     QxsjToJrsj3KMwg(qxsjsWg3km)
#
#     qxsjsCgqxzh = GetQxsjByTypeNoTb("常规气象灾害")
#     QxsjToJrsjCommon(qxsjsCgqxzh)
#     qxsjsHjqxzh = GetQxsjByTypeNoTb("环境气象灾害")
#     QxsjToJrsjCommon(qxsjsHjqxzh)
#     qxsjsDxjbzh = GetQxsjByTypeNoTb("电线积冰灾害")
#     QxsjToJrsjCommon(qxsjsDxjbzh)
#
#     qxsjsYb = GetQxsjByTypeNoTb("5KM预报网格")
#     QxsjToJrsj5KMwg(qxsjsYb)
#     qxsjsOCF = GetQxsjByTypeNoTb("OCF逐小时预报")
#     QxsjToJrsjOCF(qxsjsOCF)
#     qxsjsRadi = GetQxsjByTypeNoTb("辐射逐小时")
#     QxsjToJrsjCommon(qxsjsRadi)
#     qxsjsSCW = GetQxsjByTypeNoTb("SCW强对流预报")
#     QxsjToJrsjCommon(qxsjsSCW)
#
#     qxsjsZhyj = GetQxsjByTypeNoTb("灾害预警")
#     QxsjToJrsjAlarm(qxsjsZhyj)









