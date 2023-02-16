import pymysql
import datetime
import tarfile
import zipfile
import xml.etree.ElementTree as ET
import DbutilsPool
import json
import traceback
import jaydebeapi
import time

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


def DaquLogJXIntoMysql(type, failinfo, status, jrsjid):
    selectjrsjsql = "SELECT * FROM tb_jrsj WHERE jrsj_id = %s" % (jrsjid)
    desc = " "

    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()

        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        cursor1.execute(selectjrsjsql)
        # conn1.commit()
        jrsj = cursor1.fetchone()

        if jrsj[13] == "灾害预警":
            if status == 1:
                desc = "灾害预警接口文件" + jrsj[3] + "解析存储到tb_alarm" + "成功"
            else:
                desc = "灾害预警接口文件" + jrsj[3] + "解析存储到tb_alarm" + "失败"
                GJJLIntoMysql(desc, conn3, cursor3)
        elif jrsj[13] == "国家站小时":
            if status == 1:
                desc = "国家站小时接口文件" + jrsj[3] + "解析存储到tb_skzdzs_jx" + "成功"
            else:
                desc = "国家站小时接口文件" + jrsj[3] + "解析存储到tb_skzdzs_jx" + "失败"
                GJJLIntoMysql(desc, conn3, cursor3)
        elif jrsj[13] == "自动站小时":
            if status == 1:
                desc = "自动站小时接口文件" + jrsj[3] + "解析存储到tb_skzdzs_jx" + "成功"
            else:
                desc = "自动站小时接口文件" + jrsj[3] + "解析存储到tb_skzdzs_jx" + "失败"
                GJJLIntoMysql(desc, conn3, cursor3)
        elif jrsj[13] == "辐射逐小时":
            if status == 1:
                desc = "辐射逐小时接口文件" + jrsj[3] + "解析存储到tb_radi" + "成功"
            else:
                desc = "辐射逐小时接口文件" + jrsj[3] + "解析存储到tb_radi" + "失败"
                GJJLIntoMysql(desc, conn3, cursor3)
        elif jrsj[13] == "OCF逐小时预报":
            if status == 1:
                desc = "OCF逐小时预报接口文件" + jrsj[3] + "解析存储到tb_ocf" + "成功"
            else:
                desc = "OCF逐小时预报接口文件" + jrsj[3] + "解析存储到tb_ocf" + "失败"
                GJJLIntoMysql(desc, conn3, cursor3)
        elif jrsj[13] == "常规气象灾害":
            if status == 1:
                desc = "常规气象灾害接口文件" + jrsj[3] + "解析存储到tb_cgqxzh" + "成功"
            else:
                desc = "常规气象灾害接口文件" + jrsj[3] + "解析存储到tb_cgqxzh" + "失败"
                GJJLIntoMysql(desc, conn3, cursor3)
        elif jrsj[13] == "环境气象灾害":
            if status == 1:
                desc = "环境气象灾害接口文件" + jrsj[3] + "解析存储到tb_hjqxzh" + "成功"
            else:
                desc = "环境气象灾害接口文件" + jrsj[3] + "解析存储到tb_hjqxzh" + "失败"
                GJJLIntoMysql(desc, conn3, cursor3)
        elif jrsj[13] == "电线积冰灾害":
            if status == 1:
                desc = "电线积冰灾害接口文件" + jrsj[3] + "解析存储到tb_dxjbzh" + "成功"
            else:
                desc = "环境气象灾害接口文件" + jrsj[3] + "解析存储到tb_dzjbzh" + "失败"
                GJJLIntoMysql(desc, conn3, cursor3)
    except Exception as e:
        print("DaquLogJXIntoMysql(1) ----- ")
    finally:
        cursor1.close()
        conn1.close()
        cursor3.close()
        conn3.close()

    logtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insertlogsql = "INSERT INTO tb_daqulog(daqulog_time,daqulog_type,daqulog_failinfo,daqulog_desc,daqulog_status,daqulog_jrsjid) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",%s,%s)" % (
    logtime, type, failinfo, desc, status, jrsjid)
    server = "0"
    types = "结构化数据存储"
    insertlogsqls = "INSERT INTO tb_synclog(synclog_time,synclog_type,synclog_desc,synclog_status,synclog_server) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")" % (
    logtime, types, desc, status, server)

    try:
        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()
        cursor3.execute(insertlogsql)
        # conn3.commit()
        cursor3.execute(insertlogsqls)
        # conn3.commit()
    except Exception as e:
        print("DaquLogJXIntoMysql(2) ----- ")
    finally:
        cursor3.close()
        conn3.close()


def GJJLIntoMysql(desc, conn3, cursor3):
    try:
        logtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        gjlb = "数据同步告警"
        insertlogsql = "INSERT INTO tb_gjjl(gjjl_sj,gjjl_gjlb,gjjl_xq) VALUES (\"%s\",\"%s\",\"%s\")" % (
        logtime, gjlb, desc)
        try:
            cursor3.execute(insertlogsql)
            # conn3.commit()
        except Exception as e:
            print("GJJLIntoMysql(2) ----- ")
        finally:
            cursor3.close()
            conn3.close()
    except Exception as e:
        print("GJJLIntoMysql(1) ----- ")


def InsertJxFailLog(jrsjid, title, failinfo):
    try:
        selectjrsjsql = "SELECT * FROM tb_jrsj WHERE jrsj_id = %s" % (jrsjid)
        failtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
            cursor1 = conn1.cursor()
            cursor1.execute(selectjrsjsql)
            # conn1.commit()
            jrsj = cursor1.fetchone()
            fileinfo = "文件名：" + jrsj[3] + ";文件接入时间：" + jrsj[10].strftime('%Y-%m-%d %H:%M:%S')
            insertlogsql = "INSERT INTO tb_jxlog(jxlog_failtime,jxlog_title,jxlog_failinfo,jxlog_fileinfo) VALUES (\"%s\",\"%s\",\"%s\",\"%s\")" % (
                failtime, title, failinfo, fileinfo)
            try:
                cursor1.execute(insertlogsql)
                # conn1.commit()

                conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
                cursor2 = conn2.cursor()
                cursor2.execute(insertlogsql)
                # conn2.commit()

                conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
                cursor3 = conn3.cursor()
                cursor3.execute(insertlogsql)
                # conn3.commit()
            except Exception as e:
                print("InsertJxFailLog(3) ----- ")
            finally:
                cursor1.close()
                conn1.close()
                cursor2.close()
                conn2.close()
                cursor3.close()
                conn3.close()
        except Exception as e:
            print("InsertJxFailLog(2) ----- ")
    except Exception as e:
        print("InsertJxFailLog(1) ----- ")


# 站点逐时数据txt---解析之后存储进应用服务器的tb_skzdzs_jx 表中
def SkzdzsIntoMysql(filepath, jrsjid):
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()
        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()
        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
        elif (".TXT" in filepath):
            file = open(filepath, 'rb')
        next(file)
        StationCategory = ""
        if "_GJ_" in filepath:
            StationCategory = "国家站"
        elif "_ZD_" in filepath:
            StationCategory = "自动站"

        logstatus = 1
        failinfo = " "
        for line in file:
            try:
                line = line.decode("utf-8")
                lst = line.strip().split(',')
                # 将年月日转成2020-04-21 08:00:00标准存入数据库
                zdtime = datetime.datetime(int(lst[9]), int(lst[10]), int(lst[11]), int(lst[12]))
                # print(time)
                sql_input = "INSERT INTO tb_skzdzs_jx (skzdzs_stationname,skzdzs_province,skzdzs_city,skzdzs_cnty," \
                            "skzdzs_stationidd,skzdzs_lat,skzdzs_lon,skzdzs_time,skzdzs_prs,skzdzs_tem," \
                            "skzdzs_rhu,skzdzs_pre24,skzdzs_windavg10mi,skzdzs_winsavg10mi,skzdzs_windsmax,skzdzs_windinstmax,skzdzs_winsinstmax,skzdzs_clocov,skzdzs_stationcategory,skzdzs_jrsjid) " \
                            "VALUES ('%s', '%s', '%s', '%s', %s,%s,%s,'%s', %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,'%s',%s)" \
                            % (lst[0],lst[1],lst[2],lst[3],lst[5],lst[6],lst[7],zdtime,lst[13],lst[14],lst[15],lst[16],lst[17],lst[18],lst[19],lst[20],lst[21],lst[22],StationCategory,jrsjid)

                # cursor1.execute(sql_input)
                # conn1.commit()
                # cursor2.execute(sql_input)
                # conn2.commit()
                cursor3.execute(sql_input)
                print("tb_skzdzs_jx存储成功",filepath)
                # conn3.commit()
            except Exception as e:
                print("SkzdzsIntoMysql(2) ----- ")
                print(e)
                logstatus = 0
                type = "解析数据存储"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)
        # if logstatus != 0:
            # DaquLogJXIntoMysql("解析数据存储", "", 1, jrsjid)
        # else:
            # DaquLogJXIntoMysql("解析数据存储", failinfo, 0, jrsjid)
    except Exception as e:
        print("SkzdzsIntoMysql(1) ----- ")
        print(e)
        # title = "实况站点文件解析失败（对应文件未能解析或者未完全解析成功）"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "解析数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        # DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


# 站点逐日数据txt---解析之后存储进应用服务器的tb_skzdzr_jx 表中---以前有现在没有这个数据了，该方法不用
def SkzdzrIntoMysql(filepath, jrsjid):
    with open(filepath, 'r', encoding="utf-8") as file:
        next(file)
        StationCategory = ""
        if "_GJ_" in filepath:
            StationCategory = "国家站"
        elif "_ZD_" in filepath:
            StationCategory = "自动站"

        for line in file:
            lst = line.strip().split(',')
            # 将年月日转成2020-04-21标准存入数据库
            zdtime = datetime.date(int(lst[7]), int(lst[8]), int(lst[9]))
            # print(time)
            sql_input = "INSERT INTO tb_skzdzr_jx (skzdzr_stationname,skzdzr_province,skzdzr_country,skzdzr_city,skzdzr_cnty,skzdzr_town," \
                        "skzdzr_stationidd,skzdzr_time,skzdzr_lat,skzdzr_lon,skzdzr_prsavg,skzdzr_winsinstmax," \
                        "skzdzr_windinstmax,skzdzr_temavg,skzdzr_temmax,skzdzr_temmin,skzdzr_rhuavg,skzdzr_pretime2020,skzdzr_stationcategory,skzdzr_jrsjid) " \
                        "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s,'%s', %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,'%s',%s)" \
                        % (lst[0], lst[1], lst[2], lst[3], lst[4], lst[5], lst[6], zdtime, lst[10], lst[11]
                           , lst[12], lst[13], lst[14], lst[15], lst[16], lst[17], lst[18], lst[19], StationCategory,
                           jrsjid)
            try:
                conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
                cursor1 = conn1.cursor()
                conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
                cursor2 = conn2.cursor()
                conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
                cursor3 = conn3.cursor()

                cursor1.execute(sql_input)
                # conn1.commit()
                cursor2.execute(sql_input)
                # conn2.commit()
                cursor3.execute(sql_input)
                # conn3.commit()
            except Exception as e:
                print("SkzdzrIntoMysql ----- ")
            finally:
                cursor1.close()
                conn1.close()
                cursor2.close()
                conn2.close()
                cursor3.close()
                conn3.close()


def AlarmJxIntoMysql(ZipFilePath, jrsjid):
    try:
        # time.sleep(1)
        zipobj = zipfile.ZipFile(ZipFilePath, 'r')
        # zip压缩包下面可能有多个文件
        xmlname = ''
        for nameitem in zipobj.namelist():
            if 'xml' in nameitem:
                xmlname = nameitem
        # print(zipobj.read(zipobj.namelist()[0]).decode("utf-8"))
        # 从zip从读取xml的字符串，然后转成ElementTree元素解析
        tree = ET.fromstring(zipobj.read(xmlname).decode("utf-8"))
        root = tree
        # time.sleep(1)
        identifier = root.find("identifier").text
        sender = root.find("sender").text
        # time.sleep(1)
        senderCode = root.find("senderCode").text
        sendTime = datetime.datetime.strptime(root.find("sendTime").text, '%Y-%m-%d %H:%M:%S+08:00')
        # time.sleep(1)
        status = root.find("status").text
        msgType = root.find("msgType").text
        # time.sleep(1)
        scope = root.find("scope").text

        methodNodes = root.find("code").findall("method")
        # time.sleep(1)
        methodName = ""
        for methodNode in methodNodes:
            # time.sleep(1)
            if (methodName == "" and methodNode.find("methodName").text != None):
                methodName = methodName + methodNode.find("methodName").text
            elif (methodNode.find("methodName").text != None):
                methodName = methodName + "," + methodNode.find("methodName").text

        secClassification = root.find("secClassification").text
        note = root.find("note").text
        references = root.find("references").text
        # time.sleep(1)
        infoNode = root.find("info")
        eventType = infoNode.find("eventType").text
        # time.sleep(1)
        urgency = infoNode.find("urgency").text
        severity = infoNode.find("severity").text
        # time.sleep(1)
        certainty = infoNode.find("certainty").text
        effective = datetime.datetime.strptime(infoNode.find("effective").text, '%Y-%m-%d %H:%M:%S+08:00')
        # time.sleep(1)
        headline = infoNode.find("headline").text
        description = infoNode.find("description").text
        areaDesc = infoNode.find("area").find("areaDesc").text
        # time.sleep(1)
        polygon = infoNode.find("area").find("polygon").text
        circle = infoNode.find("area").find("circle").text
        geocode = infoNode.find("area").find("geocode").text
        # time.sleep(1)
        AlarmInfo = {
            "alarm_identifier": identifier,
            "alarm_sender": sender,
            "alarm_senderCode": senderCode,
            "alarm_sendTime": sendTime,
            "alarm_status": status,
            "alarm_msgType": msgType,
            "alarm_scope": scope,
            "alarm_methodName": methodName,
            "alarm_secClassification": secClassification,
            "alarm_note": note,
            "alarm_references": references,
            "alarm_eventType": eventType,
            "alarm_urgency": urgency,
            "alarm_severity": severity,
            "alarm_certainty": certainty,
            "alarm_effective": effective,
            "alarm_headline": headline,
            "alarm_description": description,
            "alarm_areaDesc": areaDesc,
            "alarm_polygon": polygon,
            "alarm_circle": circle,
            "alarm_geocode": geocode,
            "alarm_jrsjid": jrsjid
        }
        # print(AlarmInfo)
        aKeys = list(AlarmInfo.keys())
        aValues = list(AlarmInfo.values())
        sql = "INSERT INTO tb_alarm(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" \
              "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%s)" \
              % (
                  aKeys[0],aKeys[1],aKeys[2],aKeys[3],aKeys[4],aKeys[5],aKeys[6],aKeys[7],aKeys[8],aKeys[9],
                  aKeys[10],
                  aKeys[11],aKeys[12],aKeys[13],aKeys[14],aKeys[15],aKeys[16],aKeys[17],aKeys[18],aKeys[19],
                  aKeys[20],
                  aKeys[21],aKeys[22],
                  aValues[0],aValues[1],aValues[2],aValues[3],aValues[4],aValues[5],aValues[6],aValues[7],
                  aValues[8],
                  aValues[9],aValues[10],aValues[11],aValues[12],aValues[13],aValues[14],aValues[15],aValues[16],
                  aValues[17],aValues[18],aValues[19],aValues[20],aValues[21],jrsjid)
        print('存入内容为：',aKeys[0],aKeys[1],aKeys[2],aKeys[3],aKeys[4],aKeys[5],aKeys[6],aKeys[7],aKeys[8],aKeys[9],
                  aKeys[10],
                  aKeys[11],aKeys[12],aKeys[13],aKeys[14],aKeys[15],aKeys[16],aKeys[17],aKeys[18],aKeys[19],
                  aKeys[20],
                  aKeys[21],aKeys[22],
                  aValues[0],aValues[1],aValues[2],aValues[3],aValues[4],aValues[5],aValues[6],aValues[7],
                  aValues[8],
                  aValues[9],aValues[10],aValues[11],aValues[12],aValues[13],aValues[14],aValues[15],aValues[16],
                  aValues[17],aValues[18],aValues[19],aValues[20],aValues[21],jrsjid)
        try:
            conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
            cursor1 = conn1.cursor()
            conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
            cursor2 = conn2.cursor()
            conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
            cursor3 = conn3.cursor()
                
            cursor1.execute(sql)
            # conn1.commit()
            cursor2.execute(sql)
            # conn2.commit()
            cursor3.execute(sql)
            print('tb_alarm存入成功',aValues[0])
           # conn3.commit()
        except Exception as e:
            print("AlarmJxIntoMysql(2) ----- ",e)
        finally:
            cursor1.close()
            conn1.close()
            cursor2.close()
            conn2.close()
            cursor3.close()
            conn3.close()
        # DaquLogJXIntoMysql("解析数据存储", "", 1, jrsjid)
    except Exception as e:
        print("AlarmJxIntoMysql(1) ----- ")
        # title = "灾害预警信息解析失败（对应文件未能解析或者未完全解析成功）"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "解析数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        # DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)


# 常规气象灾害对应的txt---解析之后存储进应用服务器的tb_cgqxzh表中
def CgqxzhIntoMysql(filepath, jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
        elif (".TXT" in filepath):
            file = open(filepath, 'rb')
        next(file)

        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()
        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()
        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        logstatus = 1
        failinfo = " "
        for line in file:
            try:
                line = line.decode("utf-8")
                lst = line.strip().split(',')
                # 将datetime的str转成2020-04-21 08:00:00标准存入数据库
                zdtime = datetime.datetime.strptime(lst[5], '%Y%m%d%H%M%S')

                sql_input = "INSERT INTO tb_cgqxzh (cgqxzh_stationname,cgqxzh_province,cgqxzh_city,cgqxzh_cnty," \
                            "cgqxzh_stationidd,cgqxzh_time,cgqxzh_lat,cgqxzh_lon,cgqxzh_prsavg,cgqxzh_wins2miavg," \
                            "cgqxzh_temavg,cgqxzh_temmax,cgqxzh_temmaxotime,cgqxzh_temmin,cgqxzh_temminotime,cgqxzh_rhuavg,cgqxzh_pre2020,cgqxzh_pre0808,cgqxzh_pre2008,cgqxzh_pre0820, cgqxzh_jrsjid) " \
                            "VALUES ('%s','%s','%s','%s',%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" \
                            % (lst[0],lst[1],lst[3],lst[4],lst[6],zdtime,lst[11],lst[12],lst[13],lst[14]
                               , lst[15],lst[16],lst[17],lst[18],lst[19],lst[20],lst[21],lst[22],lst[23],
                               lst[24],
                               jrsjid)

                cursor1.execute(sql_input)
                print('11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                print('tb_cgqxzh存入成功',filepath)
                # conn1.commit()
                cursor2.execute(sql_input)
                # conn2.commit()
                cursor3.execute(sql_input)
                # conn3.commit()
            except Exception as e:
                print("CgqxzhIntoMysql(2) ----- ")
                print(e)
                logstatus = 0
                type = "解析数据存储"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)

        # if logstatus != 0:
            # DaquLogJXIntoMysql("解析数据存储", "", 1, jrsjid)
        # else:
            # DaquLogJXIntoMysql("解析数据存储", failinfo, 0, jrsjid)
    except Exception as e:
        print("CgqxzhIntoMysql(1) ----- ")
        print(e)
        # title = "常规气象灾害解析失败（对应文件未能解析或者未完全解析成功）"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "解析数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        # DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


# 环境气象灾害对应的txt---解析之后存储进应用服务器的tb_hjqxzh表中
def HjqxzhIntoMysql(filepath, jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
        elif (".TXT" in filepath):
            file = open(filepath, 'rb')
        next(file)

        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()
        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()
        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        logstatus = 1
        failinfo = " "
        for line in file:
            try:
                line = line.decode("utf-8")
                lst = line.strip().split(',')
                # print(lst)
                if len(lst) > 47:
                    # 将datetime的str转成2020-04-21 08:00:00标准存入数据库
                    zdtime = datetime.datetime.strptime(lst[5], '%Y%m%d%H%M%S')
                    if ' 'in lst[27]:

                        lst[27]=''

                    sql_input = "INSERT INTO tb_hjqxzh (hjqxzh_stationname,hjqxzh_province,hjqxzh_city,hjqxzh_cnty," \
                                "hjqxzh_stationidd,hjqxzh_time,hjqxzh_lat,hjqxzh_lon,hjqxzh_prsavg," \
                                "hjqxzh_dew,hjqxzh_frost,hjqxzh_ice,hjqxzh_smoke,hjqxzh_haze,hjqxzh_fldu,hjqxzh_flduotime," \
                                "hjqxzh_flsa,hjqxzh_flsaotime,hjqxzh_duwhr,hjqxzh_mist,hjqxzh_lit,hjqxzh_aur,hjqxzh_gawin," \
                                "hjqxzh_gss,hjqxzh_thund,hjqxzh_squa,hjqxzh_tord,hjqxzh_sast,hjqxzh_sastotime,hjqxzh_drsnow," \
                                "hjqxzh_snowst,hjqxzh_fog,hjqxzh_fogotime,hjqxzh_sori,hjqxzh_soriotime,hjqxzh_glaze,hjqxzh_glazeotime,hjqxzh_rain," \
                                "hjqxzh_preotime,hjqxzh_snow,hjqxzh_snowotime,hjqxzh_hail,hjqxzh_hailotime,hjqxzh_jrsjid) " \
                                "VALUES ('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                "%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                                % (lst[0],lst[1],lst[3],lst[4],lst[6],zdtime,lst[11],lst[12],lst[13],lst[14],
                                   lst[15],lst[16],lst[17],lst[18],lst[19],lst[20],lst[21],lst[22],lst[23],
                                   lst[25],lst[25],lst[26],'0',lst[28],lst[29],lst[30],lst[31],'0',lst[33],'0',
                                   lst[35],'0',lst[37],lst[38],lst[39],'0',lst[41],'0',lst[43],
                                   lst[44],lst[45],lst[46],lst[47],jrsjid)
                    # print(sql_input)

                    cursor1.execute(sql_input)
                    print('111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                    print('tb_hjqxzh',filepath)
                    # conn1.commit()
                    cursor2.execute(sql_input)
                    # conn2.commit()
                    cursor3.execute(sql_input)
                    # conn3.commit()
            except Exception as e:
                print("HjqxzhIntoMysql(2) ----- ")
                print(e)
                logstatus = 0
                type = "解析数据存储"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)

        # if logstatus != 0:
            # DaquLogJXIntoMysql("解析数据存储", "", 1, jrsjid)
        # else:
            # DaquLogJXIntoMysql("解析数据存储", failinfo, 0, jrsjid)
    except Exception as e:
        print("HjqxzhIntoMysql(1) ----- ")
        print(e)
        # print(traceback.format_exc())
        # title = "环境气象灾害文件解析失败（对应文件未能解析或者未完全解析成功）"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo+"," +str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "解析数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        # DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


# 电线积冰灾害对应的txt---解析之后存储进应用服务器的tb_dxjbzh表中
def DxjbzhIntoMysql(filepath, jrsjid):
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()
        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()
        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
        elif (".TXT" in filepath):
            file = open(filepath, 'rb')
        next(file)

        logstatus = 1
        failinfo = " "
        for line in file:
            try:
                line = line.decode("utf-8")
                lst = line.strip().split(',')
                # 将datetime的str转成2020-04-21 08:00:00标准存入数据库
                zdtime = datetime.datetime.strptime(lst[5], '%Y%m%d%H%M%S')

                sql_input = "INSERT INTO tb_dxjbzh (dxjbzh_stationname,dxjbzh_province,dxjbzh_city,dxjbzh_cnty," \
                            "dxjbzh_stationidd,dxjbzh_time,dxjbzh_lat,dxjbzh_lon,dxjbzh_eice,dxjbzh_eicetns," \
                            "dxjbzh_eicetwe,dxjbzh_eicewns,dxjbzh_eicewwe,dxjbzh_eicedns,dxjbzh_eicedwe,dxjbzh_jrsjid) " \
                            "VALUES ('%s', '%s', '%s', '%s', %s,'%s',%s,%s, %s, %s,%s,%s,%s,%s,%s,%s)" \
                            % (lst[0], lst[1], lst[3], lst[4], lst[6], zdtime, lst[11], lst[12], lst[13], lst[14]
                               , lst[15], lst[16], lst[17], lst[18], lst[19], jrsjid)

                cursor1.execute(sql_input)
                print('111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                print('tb_dxjbzh存入成功',filepath)
                # conn1.commit()
                cursor2.execute(sql_input)
                # conn2.commit()
                cursor3.execute(sql_input)
                # conn3.commit()
            except Exception as e:
                print("DxjbzhIntoMysql(2) ----- ")
                print(e)
                logstatus = 0
                type = "解析数据存储"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)

        # if logstatus != 0:
            # DaquLogJXIntoMysql("解析数据存储", "", 1, jrsjid)
        # else:
            # DaquLogJXIntoMysql("解析数据存储", failinfo, 0, jrsjid)
    except Exception as e:
        print("DxjbzhIntoMysql(1) ----- ")
        print(e)
        # title = "电线积冰灾害文件解析失败（对应文件未能解析或者未完全解析成功）"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "解析数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        # DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


# 辐射逐小时对应的txt---解析之后存储进应用服务器的tb_cgqxzh 表中
def RadiIntoMysql(filepath, jrsjid):
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()
        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()
        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        try:
            if ".tar.gz" in filepath:
                TarFile = tarfile.open(filepath, 'r')
                file = (TarFile.extractfile(TarFile.firstmember))
            elif (".TXT" in filepath):
                file = open(filepath, 'rb')
        except Exception as e:
            print("RadiIntoMysql(4) ----- ")
        finally:
            next(file)

            logstatus = 1
            failinfo = " "
            for line in file:
                try:
                    line = line.decode("utf-8")
                    lst = line.strip().split(',')
                    # 将datetime的str转成2020-04-21 08:00:00标准存入数据库
                    # print(lst[4])
                    zdtime = datetime.datetime.strptime(lst[4], '%Y%m%d%H%M%S')
                    sql_input = "INSERT INTO tb_radi (radi_stationname, radi_province, radi_city, radi_cnty," \
                                "radi_stationidd, radi_time, radi_lat,radi_lon,radi_alti,radi_v14311,radi_v14320,radi_jrsjid) " \
                                "VALUES ('%s', '%s', '%s', '%s', %s,'%s',%s,%s, %s,%s, %s, %s)" \
                                % (lst[0], lst[1], lst[2], lst[3], lst[5], zdtime, lst[7], lst[8], lst[9], lst[15],
                                   lst[16],
                                   jrsjid)

                    try:
                        cursor1.execute(sql_input)
                        print('1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                        print('tb_radi存入成功',filepath)
                        # conn1.commit()
                        cursor2.execute(sql_input)
                        # conn2.commit()
                        cursor3.execute(sql_input)
                        # conn3.commit()
                    except Exception as e:
                        print("RadiIntoMysql(3) ----- ")

                except Exception as e:
                    print("RadiIntoMysql(2) ----- ")
                    logstatus = 0
                    type = "解析数据存储"
                    failinfo = ""
                    for each in e.args:
                        if len(failinfo) == 0:
                            failinfo = failinfo + str(each)
                        else:
                            failinfo = failinfo + "," + str(each)

            # if logstatus != 0:
                # DaquLogJXIntoMysql("解析数据存储", "", 1, jrsjid)
            # else:
                # DaquLogJXIntoMysql("解析数据存储", failinfo, 0, jrsjid)
    except Exception as e:
        print("RadiIntoMysql(1) ----- ",e)
        # title = "辐射逐小时文件解析失败（对应文件未能解析或者未完全解析成功）"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "解析数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        # DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


# OCF逐小时预报json文件数据---解析之后存储进应用服务器的tb_ocf 表中
def OCFIntoMysql(filepath, jrsjid):
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()

        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()

        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()

        try:
            if ".tar.gz" in filepath:
                TarFile = tarfile.open(filepath, 'r')
                file = (TarFile.extractfile(TarFile.firstmember))
            elif (".json" in filepath):
                file = open(filepath, 'rb')
        except Exception as e:
            print("OCFIntoMysql(4) ----- ")
        finally:
            jsondata = json.load(file)
            ocfdata = jsondata['data']

            logstatus = 1
            failinfo = " "
            for key in ocfdata:
                try:
                    # print(ocfdata[key][0]['cityinfo'])
                    areaid = ocfdata[key][0]['cityinfo']['areaid']  # 9位站点号
                    city = ocfdata[key][0]['cityinfo']['city']  # 市
                    country = ocfdata[key][0]['cityinfo']['country']  # 国家
                    lat = ocfdata[key][0]['cityinfo']['lat']  # 纬度
                    lon = ocfdata[key][0]['cityinfo']['lon']  # 经度
                    namecn = ocfdata[key][0]['cityinfo']['namecn']  # 站点名称
                    province = ocfdata[key][0]['cityinfo']['province']  # 省份
                    time = ocfdata[key][0]['cityinfo']['time']  # 更新时间
                    time_zone = ocfdata[key][0]['cityinfo']['time_zone']  # 北京时间
                    zone_abb = ocfdata[key][0]['cityinfo']['zone_abb']  # 标准时区
                    j0 = ocfdata[key][1]['j']['j0']  #
                    # print(areaid +","+city +","+country +","+lat +","+lon +","+namecn +","+province +","+time +","+time_zone +","+zone_abb +","+j0)
                    j1list = ocfdata[key][1]['j']['j1']
                    for item in j1list:
                        ja = item['ja']  # 天气现象编码
                        jb = item['jb']  # 风向编号
                        jc = item['jc']  # 风力编号
                        jd = item['jd']  # 0无降水，1降雨，2降雪
                        je = item['je']  # 累计降水量
                        jf = item['jf']  # 温度
                        jg = item['jg']  # 相对湿度
                        jh = item['jh']  # 云量
                        ji = item['ji']  # 预报时间（北京时）
                        jk = item['jk']  # 360度风向
                        jl = item['jl']  # 16方位风向
                        jm = item['jm']  # 风速大小
                        # print(ja + "," + jb + "," + jc + "," + jd + "," + je + "," + jf + "," + jg + "," + jh + "," + ji + "," + str(jk )+ "," + jl+ "," + str(jm))

                        sql_input = "INSERT INTO tb_ocf (ocf_areaid,ocf_city,ocf_country,ocf_lat,ocf_lon,ocf_namecn,ocf_province,ocf_time,ocf_timezone,ocf_zoneabb,ocf_j0," \
                                    "ocf_ja,ocf_jb,ocf_jc,ocf_jd,ocf_je,ocf_jf,ocf_jg,ocf_jh,ocf_ji,ocf_jk,ocf_jl,ocf_jm,ocf_jrsjid)" \
                                    "VALUES ('%s','%s','%s',%s,%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,%s,%s,%s,%s,'%s',%s,'%s',%s,%s)" \
                                    % (areaid,city,country,lat,lon,namecn,province,time,time_zone,zone_abb,j0,
                                       ja,jb,jc,jd,je,jf,jg,jh,ji,jk,jl,jm,jrsjid)

                        try:
                            cursor1.execute(sql_input)
                            print('tb_ocf存入成功1111111111111111111111111',filepath)
                            # conn1.commit()
                            cursor2.execute(sql_input)
                            print('2222222222222222222')
                            # conn2.commit()
                            cursor3.execute(sql_input)
                            print('3333333333333333333333')
                            # conn3.commit()
                        except Exception as e:
                            print("OCFIntoMysql(3) ----- ")

                except Exception as e:
                    print("OCFIntoMysql(2) ----- ")
                    logstatus = 0
                    type = "解析数据存储"
                    failinfo = ""
                    for each in e.args:
                        if len(failinfo) == 0:
                            failinfo = failinfo + str(each)
                        else:
                            failinfo = failinfo + "," + str(each)

            # if logstatus != 0:
                # DaquLogJXIntoMysql("解析数据存储", "", 1, jrsjid)
            # else:
                # DaquLogJXIntoMysql("解析数据存储", failinfo, 0, jrsjid)
    except Exception as e:
        print("OCFIntoMysql(1) ----- ")
        # title = "OCF逐小时预报解析失败（对应文件未能解析或者未完全解析成功）"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "解析数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        # DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


def SCWJxIntoMysql(filepath, jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
            filenames = TarFile.getnames()
            fileContent = file.read().decode("utf-8")

            for item in filenames:
                if ".json" in item:
                    file = (TarFile.extractfile(item))
                    filename = item
                    if len(fileContent):
                        jsonObj = json.loads(fileContent)
                        for key in jsonObj:
                            if key == 'startTime':
                                startTimeStr = jsonObj[key]
                                startTime = startTimeStr[0:4] + '-' + startTimeStr[4:6] + '-' + startTimeStr[
                                                                                                6:8] + ' ' + startTimeStr[
                                                                                                             8:10] + ':' + startTimeStr[
                                                                                                                           10:12] + ':00'
                            if key == 'endTime':
                                endTimeStr = jsonObj[key]
                                endTime = endTimeStr[0:4] + '-' + endTimeStr[4:6] + '-' + endTimeStr[
                                                                                          6:8] + ' ' + endTimeStr[
                                                                                                       8:10] + ':' + endTimeStr[
                                                                                                                     10:12] + ':00'
                            if key == 'title':
                                title = jsonObj[key]
                            if key == 'forecast':
                                # 强对流外推
                                forecast = jsonObj[key]
                        for key in jsonObj:
                            if key == 'obs':
                                # 强对流实况分析
                                obs = jsonObj[key]
                                for el in obs:
                                    # 短时强降水
                                    if el == 'rain':
                                        rain = obs[el]
                                        getScwLocation(rain, el, startTime, endTime, jrsjid,filepath)
                                    # 雷雨大风
                                    if el == 'wind':
                                        wind = obs[el]
                                        getScwLocation(wind, el, startTime, endTime, jrsjid,filepath)
                                    # 冰雹
                                    if el == 'hail':
                                        hail = obs[el]
                                        getScwLocation(hail, el, startTime, endTime, jrsjid,filepath)
                                    # 短时强降水和雷雨大风
                                    if el == 'rain_wind':
                                        rain_wind = obs[el]
                                        getScwLocation(rain_wind, el, startTime, endTime, jrsjid,filepath)
                                    # 短时强降水和冰雹
                                    if el == 'rain_hail':
                                        rain_hail = obs[el]
                                        getScwLocation(rain_hail, el, startTime, endTime, jrsjid,filepath)
                                    # 雷雨大风和冰雹
                                    if el == 'wind_hail':
                                        wind_hail = obs[el]
                                        getScwLocation(wind_hail, el, startTime, endTime, jrsjid,filepath)
                                    # 短时强降水、雷雨大风和冰雹
                                    if el == 'rain_wind_hail':
                                        rain_wind_hail = obs[el]
                                        getScwLocation(rain_wind_hail, el, startTime, endTime, jrsjid,filepath)

            TarFile.close()
    except Exception as e:
        print("SCWJxIntoMysql(1) ----- " + str(e))


# 解析scw数据的坐标
def getScwLocation(data, elName, startTime, endTime, jrsjid,filepath):
    # 循环落区数据
    try:
        conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor1 = conn1.cursor()

        conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
        cursor2 = conn2.cursor()

        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()
        conn = pymysql.connect(host= 'localhost',db='dlqxsync',user='root',password='root',charset='utf8',port=13307)
        cursor=conn.cursor()
        tup = []
        for item in data:
            #time.sleep(1)
            for key in item:
                # time.sleep(1)
                # 强对流的强度(字段待用)
                if key == 'intensity':
                    intensity = item[key]
            for key in item:
                # time.sleep(1)
                # 经纬度数据
                if key == 'xy':
                    xy = item[key]
                    lat = []
                    lon = []
                    for index in range(len(xy)):
                        # time.sleep(1)
                        # 第1个值是纬度
                        if index % 2 == 0:
                            lat.append(xy[index])
                        # 第2个值是经度
                        else:
                            lon.append(xy[index])
                    # 写入数据到mysql
                    for index in range(len(lat)):
                        # time.sleep(1)
                        sql = "INSERT INTO tb_scw (scw_lat,scw_lon,scw_type,scw_start,scw_end,scw_intensity,scw_jrsjid) VALUES ('%s','%s','%s','%s','%s','%s','%s')" % (
                            lat[index],lon[index],elName,startTime,endTime,intensity,jrsjid)
                        try:
                            print('111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                            cursor.execute(sql)
                            conn.commit()
                            print('tb_scw存入成功',filepath)
                            # cursor2.execute(sql)
                            # conn2.commit()
                            # cursor3.execute(sql)
                            # conn3.commit()
                        except Exception as e:
                            print("getScwLocation(2) ----- " + str(e))
    except Exception as e:
        print("getScwLocation(1) ----- " + str(e))
    finally:
        cursor.close()
        conn.close()
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


def JrsjSkzszsJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            SkzdzsIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjSkzszsJxIntoMysql ----- ")
        print(e)


def JrsjSkzszrJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            SkzdzrIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjSkzszrJxIntoMysql ----- ")
        print(e)


def JrsjAlarmJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            # filepath = item[12].split(".")[0] + ".xml"
            filepath = item[12]
            print('filepath:',item[12])
            jrsjid = item[0]
            print('jrsjid:',item[0])
            AlarmJxIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjAlarmJxIntoMysql ----- ")
        print(e)


def JrsjCgqxzhJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            CgqxzhIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjCgqxzhJxIntoMysql ----- ")
        print(e)


def JrsjHjqxzhJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            HjqxzhIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjHjqxzhJxIntoMysql ----- ")
        print(e)


def JrsjDxjbzhIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            DxjbzhIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjDxjbzhIntoMysql ----- ")
        print(e)


def JrsjRadiJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            RadiIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjRadiJxIntoMysql ----- ")
        print(e)


def JrsjSCWJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            SCWJxIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjSCWJxIntoMysql ----- " + str(e))


def JrsjOCFJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            OCFIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjOCFJxIntoMysql ----- ")
        print(e)


# jrsj_jxstorestatus：标志tb_jrsj接口数据是否解析存储到mysql表中，默认为0,0标志未同步，1标志该项已经同步
def GetJrsjByTypeNoJxStore(qxsjtype):
    jrsj = []
    try:

        sql = "SELECT * FROM tb_jrsj WHERE jrsj_jxstorestatus = 0 and jrsj_qxtype = '%s'" % (qxsjtype)
        try:
            conn = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
            cursor = conn.cursor()
            cursor.execute(sql)
            # conn.commit()
            jrsj = cursor.fetchall()
        except Exception as e:
            print("GetJrsjByTypeNoJxStore(2) ----- ")
            print(e)
        finally:
            return jrsj
            cursor.close()
            conn.close()
    except Exception as e:
        print("GetJrsjByTypeNoJxStore(1) ----- ")


# 数据存储成功之后使用：将jrsj_jxstorestatus标志未改为1
def SetJrsjJxStatus1(qxsjs):
    try:
        conn = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor = conn.cursor()
        for item in qxsjs:
            sql = "UPDATE  tb_jrsj SET jrsj_jxstorestatus = 1 WHERE jrsj_id = %s " % (item[0])
            try:
                cursor.execute(sql)
                # conn.commit()
            except Exception as e:
                print("SetJrsjJxStatus1(2) ----- ")
                print(e)
    except Exception as e:
        print("SetJrsjJxStatus1(1) ----- ")
        print(e)
    finally:
        cursor.close()
        conn.close()

# if __name__ == '__main__':
#     HjqxzhIntoMysql("F:/lscf/HJ_QXZH_20160129000000.tar.gz",8035)
# jrsjgjzd = GetJrsjByTypeNoJxStore("国家站小时")
# JrsjSkzszsJxIntoMysql(jrsjgjzd)
# jrsjzdzd = GetJrsjByTypeNoJxStore("自动站小时")
# JrsjSkzszsJxIntoMysql(jrsjzdzd)
# jrsj = GetJrsjByTypeNoJxStore("辐射逐小时")
# JrsjRadiJxIntoMysql(jrsj)
# jrsjalarm = GetJrsjByTypeNoJxStore("灾害预警")
# JrsjAlarmJxIntoMysql(jrsjalarm)
# jrsjCgqxzh = GetJrsjByTypeNoJxStore("常规气象灾害")
# JrsjCgqxzhJxIntoMysql(jrsjCgqxzh)
# jrsjHjqxzh = GetJrsjByTypeNoJxStore("环境气象灾害")
# JrsjHjqxzhJxIntoMysql(jrsjHjqxzh)
# jrsjDxjbzh = GetJrsjByTypeNoJxStore("电线积冰灾害")
# JrsjDxjbzhIntoMysql(jrsjDxjbzh)
# jrsjOCF= GetJrsjByTypeNoJxStore("OCF逐小时预报")
# JrsjOCFJxIntoMysql(jrsjOCF)


# SetJrsjJxStatus1(jrsj)

