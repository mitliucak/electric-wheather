import pymysql
import datetime
import tarfile
import zipfile
import xml.etree.ElementTree as ET
import DbutilsPool
import json
import traceback
import jaydebeapi
import jpype
import ConfigParam

# jar_file = 'sgjdbc_4.3.18.1_20200115.jar'
# driver = 'sgcc.nds.jdbc.driver.NdsDriver'
# jdbc_url1 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqxsync_13306?appname=app_dlqxsync_13306&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# jdbc_url2 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqxsync_13307?appname=app_dlqxsync_13307&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# jdbc_url3 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqx_zl?appname=app_dlqx_zl&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# uandp = ["root", "shanxi_QX"]

jar_file = 'mysql-connector-java-8.0.11.jar'
driver = 'com.mysql.cj.jdbc.Driver'
#jdbc_url1 = 'jdbc:mysql://121.52.212.109:13306/06dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
#jdbc_url2 = 'jdbc:mysql://121.52.212.109:13307/07dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
#jdbc_url3 = 'jdbc:mysql://121.52.212.109:13308/08dlqx_zl?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
#uandp = ["root", "123456"]
jdbc_url1 = ConfigParam.db_url['jdbc_url1']
jdbc_url2 = ConfigParam.db_url['jdbc_url2']
jdbc_url3 = ConfigParam.db_url['jdbc_url3']
uandp = ConfigParam.db_url['uandp']


def JrsjSCWJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            SCWJxIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjSCWJxIntoMysql ----- " + str(e))

def DaquLogJXIntoMysql(type,failinfo,status,jrsjid):

    selectjrsjsql = "SELECT * FROM tb_jrsj WHERE jrsj_id = %s" % (jrsjid)
    desc =" "

    try:
        conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
        cursor1 = conn1.cursor()
        # conn2 = DbutilsPool.get_db_pool2(2)
        # cursor2 = conn2.cursor()
        conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
        cursor3 = conn3.cursor()

        cursor1.execute(selectjrsjsql)
        jrsj = cursor1.fetchone()

        if jrsj[13] == "????????????":
            if status == 1:
                desc = "????????????????????????"+jrsj[3]+"???????????????tb_alarm"+"??????"
            else:
                desc = "????????????????????????" + jrsj[3] + "???????????????tb_alarm" + "??????"
                GJJLIntoMysql(desc,conn3,cursor3)
        elif jrsj[13] == "???????????????":
            if status == 1:
                desc = "???????????????????????????"+jrsj[3]+"???????????????tb_skzdzs_jx"+"??????"
            else:
                desc = "???????????????????????????" + jrsj[3] + "???????????????tb_skzdzs_jx" + "??????"
                GJJLIntoMysql(desc,conn3,cursor3)
        elif jrsj[13] == "???????????????":
            if status == 1:
                desc = "???????????????????????????"+jrsj[3]+"???????????????tb_skzdzs_jx"+"??????"
            else:
                desc = "???????????????????????????" + jrsj[3] + "???????????????tb_skzdzs_jx" + "??????"
                GJJLIntoMysql(desc,conn3,cursor3)
        elif jrsj[13] == "???????????????":
            if status == 1:
                desc = "???????????????????????????"+jrsj[3]+"???????????????tb_radi"+"??????"
            else:
                desc = "???????????????????????????" + jrsj[3] + "???????????????tb_radi" + "??????"
                GJJLIntoMysql(desc,conn3,cursor3)
        elif jrsj[13] == "OCF???????????????":
            if status == 1:
                desc = "OCF???????????????????????????"+jrsj[3]+"???????????????tb_ocf"+"??????"
            else:
                desc = "OCF???????????????????????????" + jrsj[3] + "???????????????tb_ocf" + "??????"
                GJJLIntoMysql(desc,conn3,cursor3)
        elif jrsj[13] == "??????????????????":
            if status == 1:
                desc = "??????????????????????????????"+jrsj[3]+"???????????????tb_cgqxzh"+"??????"
            else:
                desc = "??????????????????????????????" + jrsj[3] + "???????????????tb_cgqxzh" + "??????"
                GJJLIntoMysql(desc,conn3,cursor3)
        elif jrsj[13] == "??????????????????":
            if status == 1:
                desc = "??????????????????????????????"+jrsj[3]+"???????????????tb_hjqxzh"+"??????"
            else:
                desc = "??????????????????????????????" + jrsj[3] + "???????????????tb_hjqxzh" + "??????"
                GJJLIntoMysql(desc,conn3,cursor3)
        elif jrsj[13] == "??????????????????":
            if status == 1:
                desc = "??????????????????????????????"+jrsj[3]+"???????????????tb_dxjbzh"+"??????"
            else:
                desc = "??????????????????????????????" + jrsj[3] + "???????????????tb_dzjbzh" + "??????"
                GJJLIntoMysql(desc,conn3,cursor3)
    except Exception as e:
        print("DaquLogJXIntoMysql(1) ----- ")
    finally:
        cursor1.close()
        conn1.close()
        cursor3.close()
        conn3.close()

    logtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insertlogsql = "INSERT INTO tb_daqulog(daqulog_time,daqulog_type,daqulog_failinfo,daqulog_desc,daqulog_status,daqulog_jrsjid) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",%s,%s)" % (logtime, type, failinfo, desc,status,jrsjid)
    server = "0"
    types ="?????????????????????"
    insertlogsqls = "INSERT INTO tb_synclog(synclog_time,synclog_type,synclog_desc,synclog_status,synclog_server) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")" %  (logtime, types , desc,status,server)
    
    try:
        conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
        cursor3 = conn3.cursor()
        cursor3.execute(insertlogsql)
        cursor3.execute(insertlogsqls)
    except Exception as e:
        print("DaquLogJXIntoMysql(2) ----- ")
    finally:
        cursor3.close()
        conn3.close()


def GJJLIntoMysql(desc,conn3,cursor3):
    try:    
        logtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        gjlb ="??????????????????" 
        insertlogsql = "INSERT INTO tb_gjjl(gjjl_sj,gjjl_gjlb,gjjl_xq) VALUES (\"%s\",\"%s\",\"%s\")" %  (logtime, gjlb , desc)
        try:
            cursor3.execute(insertlogsql)
        except Exception as e:
            print("GJJLIntoMysql(2) ----- ")
        finally:
            cursor3.close()
            conn3.close()
    except Exception as e:
        print("GJJLIntoMysql(1) ----- ")

def InsertJxFailLog(jrsjid,title,failinfo):
    try:
        selectjrsjsql = "SELECT * FROM tb_jrsj WHERE jrsj_id = %s" % (jrsjid)
        failtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor1 = conn1.cursor()
            cursor1.execute(selectjrsjsql)
            jrsj = cursor1.fetchone()
            fileinfo = "????????????"+jrsj[3]+";?????????????????????"+jrsj[10].strftime('%Y-%m-%d %H:%M:%S')
            insertlogsql = "INSERT INTO tb_jxlog(jxlog_failtime,jxlog_title,jxlog_failinfo,jxlog_fileinfo) VALUES (\"%s\",\"%s\",\"%s\",\"%s\")" % (
            failtime, title, failinfo, fileinfo)
            try:
                cursor1.execute(insertlogsql)

                conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
                cursor2 = conn2.cursor()
                cursor2.execute(insertlogsql)

                conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
                cursor3 = conn3.cursor()
                cursor3.execute(insertlogsql)
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

#??????????????????txt---???????????????????????????????????????tb_skzdzs_jx ??????
def SkzdzsIntoMysql(filepath,jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
        elif(".TXT" in filepath):
            file = open(filepath, 'rb')
        next(file)
        StationCategory = ""
        if "_GJ_" in filepath:
            StationCategory = "?????????"
        elif "_ZD_" in filepath:
            StationCategory = "?????????"

        logstatus = 1
        failinfo = " "
        for line in file:
            try:
                line = line.decode("utf-8")
                lst = line.strip().split(',')
                # ??????????????????2020-04-21 08:00:00?????????????????????
                zdtime = datetime.datetime(int(lst[9]), int(lst[10]), int(lst[11]),int(lst[12]))
                # print(time)
                sql_input = "INSERT INTO tb_skzdzs_jx (skzdzs_stationname,skzdzs_province,skzdzs_city,skzdzs_cnty," \
                            "skzdzs_stationidd,skzdzs_lat,skzdzs_lon,skzdzs_time,skzdzs_prs,skzdzs_tem," \
                            "skzdzs_rhu,skzdzs_pre24,skzdzs_windavg10mi,skzdzs_winsavg10mi,skzdzs_windsmax,skzdzs_windinstmax,skzdzs_winsinstmax,skzdzs_clocov,skzdzs_stationcategory,skzdzs_jrsjid) " \
                            "VALUES ('%s', '%s', '%s', '%s', %s,%s,%s,'%s', %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,'%s',%s)" \
                            % (lst[0], lst[1], lst[2], lst[3], lst[5], lst[6], lst[7], zdtime
                               , lst[13], lst[14], lst[15], lst[16], lst[17], lst[18], lst[19], lst[20], lst[21],lst[22],
                               StationCategory, jrsjid)

                conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                cursor1 = conn1.cursor()
                conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
                cursor2 = conn2.cursor()
                conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
                cursor3 = conn3.cursor()

                cursor1.execute(sql_input)
                cursor2.execute(sql_input)
                cursor3.execute(sql_input)
            except Exception as e:
                print("SkzdzsIntoMysql(2) ----- ")
                logstatus = 0
                type = "??????????????????"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)
            finally:
                cursor1.close()
                conn1.close()
                cursor2.close()
                conn2.close()
                cursor3.close()
                conn3.close()
        if logstatus != 0:
            DaquLogJXIntoMysql("??????????????????", "", 1, jrsjid)
        else:
            DaquLogJXIntoMysql("??????????????????", failinfo, 0, jrsjid)
    except Exception as e:
        print("SkzdzsIntoMysql(1) ----- ")
        # title = "???????????????????????????????????????????????????????????????????????????????????????"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "??????????????????"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)

#??????????????????txt---???????????????????????????????????????tb_skzdzr_jx ??????---??????????????????????????????????????????????????????
def SkzdzrIntoMysql(filepath,jrsjid):
    with open(filepath,'r',encoding="utf-8") as file:
        next(file)
        StationCategory = ""
        if "_GJ_" in filepath:
            StationCategory = "?????????"
        elif "_ZD_" in filepath:
            StationCategory = "?????????"

        for line in file:
            lst = line.strip().split(',')
            #??????????????????2020-04-21?????????????????????
            zdtime = datetime.date(int(lst[7]), int(lst[8]), int(lst[9]))
            # print(time)
            sql_input = "INSERT INTO tb_skzdzr_jx (skzdzr_stationname,skzdzr_province,skzdzr_country,skzdzr_city,skzdzr_cnty,skzdzr_town," \
                        "skzdzr_stationidd,skzdzr_time,skzdzr_lat,skzdzr_lon,skzdzr_prsavg,skzdzr_winsinstmax," \
                        "skzdzr_windinstmax,skzdzr_temavg,skzdzr_temmax,skzdzr_temmin,skzdzr_rhuavg,skzdzr_pretime2020,skzdzr_stationcategory,skzdzr_jrsjid) " \
                        "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s,'%s', %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,'%s',%s)" \
                        % (lst[0],lst[1],lst[2],lst[3],lst[4],lst[5],lst[6],zdtime,lst[10],lst[11]
                           ,lst[12], lst[13],lst[14],lst[15],lst[16],lst[17],lst[18],lst[19],StationCategory,jrsjid)
            try:
                conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                cursor1 = conn1.cursor()
                conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
                cursor2 = conn2.cursor()
                conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
                cursor3 = conn3.cursor()

                cursor1.execute(sql_input)
                cursor2.execute(sql_input)
                cursor3.execute(sql_input)
            except Exception as e:
                print("SkzdzrIntoMysql ----- ")
            finally:
                cursor1.close()
                conn1.close()
                cursor2.close()
                conn2.close()
                cursor3.close()
                conn3.close()

def AlarmJxIntoMysql(ZipFilePath,jrsjid):
    try:
        zipobj = zipfile.ZipFile(ZipFilePath, 'r')
        #zip????????????????????????????????????
        xmlname = ''
        for nameitem in zipobj.namelist():
            if 'xml' in nameitem:
                xmlname = nameitem
        # print(zipobj.read(zipobj.namelist()[0]).decode("utf-8"))
        # ???zip?????????xml???????????????????????????ElementTree????????????
        tree = ET.fromstring(zipobj.read(xmlname).decode("utf-8"))
        root = tree
        identifier = root.find("identifier").text
        sender = root.find("sender").text
        senderCode = root.find("senderCode").text
        sendTime = datetime.datetime.strptime(root.find("sendTime").text, '%Y-%m-%d %H:%M:%S+08:00')
        status = root.find("status").text
        msgType = root.find("msgType").text
        scope = root.find("scope").text

        methodNodes = root.find("code").findall("method")
        methodName = ""
        for methodNode in methodNodes:
            if (methodName == "" and methodNode.find("methodName").text != None):
                methodName = methodName + methodNode.find("methodName").text
            elif (methodNode.find("methodName").text != None):
                methodName = methodName + "," + methodNode.find("methodName").text

        secClassification = root.find("secClassification").text
        note = root.find("note").text
        references = root.find("references").text

        infoNode = root.find("info")
        eventType = infoNode.find("eventType").text
        urgency = infoNode.find("urgency").text
        severity = infoNode.find("severity").text
        certainty = infoNode.find("certainty").text
        effective = datetime.datetime.strptime(infoNode.find("effective").text , '%Y-%m-%d %H:%M:%S+08:00')
        headline = infoNode.find("headline").text
        description = infoNode.find("description").text
        areaDesc = infoNode.find("area").find("areaDesc").text
        polygon = infoNode.find("area").find("polygon").text
        circle = infoNode.find("area").find("circle").text
        geocode = infoNode.find("area").find("geocode").text
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
        sql = "INSERT INTO tb_alarm(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)" \
              "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s',%s)" \
              % (
              aKeys[0], aKeys[1], aKeys[2], aKeys[3], aKeys[4], aKeys[5], aKeys[6], aKeys[7], aKeys[8], aKeys[9], aKeys[10],
              aKeys[11], aKeys[12], aKeys[13], aKeys[14], aKeys[15], aKeys[16], aKeys[17], aKeys[18], aKeys[19], aKeys[20],
              aKeys[21], aKeys[22],
              aValues[0], aValues[1], aValues[2], aValues[3], aValues[4], aValues[5], aValues[6], aValues[7], aValues[8],
              aValues[9], aValues[10], aValues[11], aValues[12], aValues[13], aValues[14], aValues[15], aValues[16],
              aValues[17], pymysql.escape_string(aValues[18]), aValues[19], aValues[20], pymysql.escape_string(aValues[21]),jrsjid)
        
        try:
            conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor1 = conn1.cursor()
            conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
            cursor2 = conn2.cursor()
            conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
            cursor3 = conn3.cursor()

            cursor1.execute(sql)
            cursor2.execute(sql)
            cursor3.execute(sql)
        except Exception as e:
            print("AlarmJxIntoMysql(2) ----- ")
        finally:
            cursor1.close()
            conn1.close()
            cursor2.close()
            conn2.close()
            cursor3.close()
            conn3.close()
        DaquLogJXIntoMysql("??????????????????", "", 1, jrsjid)
    except Exception as e:
        print("AlarmJxIntoMysql(1) ----- ")
        # title = "???????????????????????????????????????????????????????????????????????????????????????"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "??????????????????"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)


#???????????????????????????txt---???????????????????????????????????????tb_cgqxzh??????
def CgqxzhIntoMysql(filepath,jrsjid):
    try:
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
                # ???datetime???str??????2020-04-21 08:00:00?????????????????????
                zdtime = datetime.datetime.strptime(lst[5], '%Y%m%d%H%M%S')

                sql_input = "INSERT INTO tb_cgqxzh (cgqxzh_stationname,cgqxzh_province,cgqxzh_city,cgqxzh_cnty," \
                            "cgqxzh_stationidd,cgqxzh_time,cgqxzh_lat,cgqxzh_lon,cgqxzh_prsavg,cgqxzh_wins2miavg," \
                            "cgqxzh_temavg,cgqxzh_temmax,cgqxzh_temmaxotime,cgqxzh_temmin,cgqxzh_temminotime,cgqxzh_rhuavg,cgqxzh_pre2020,cgqxzh_pre0808,cgqxzh_pre2008,cgqxzh_pre0820, cgqxzh_jrsjid) " \
                            "VALUES ('%s', '%s', '%s', '%s', %s,'%s',%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" \
                            % (lst[0], lst[1], lst[3], lst[4], lst[6], zdtime, lst[11], lst[12], lst[13], lst[14]
                               , lst[15], lst[16], lst[17], lst[18], lst[19], lst[20], lst[21], lst[22], lst[23], lst[24],
                               jrsjid)
                conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                cursor1 = conn1.cursor()
                conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
                cursor2 = conn2.cursor()
                conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
                cursor3 = conn3.cursor()

                cursor1.execute(sql_input)
                cursor2.execute(sql_input)
                cursor3.execute(sql_input)
            except Exception as e:
                print("CgqxzhIntoMysql(2) ----- ")
                logstatus = 0
                type = "??????????????????"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)
            finally:
                cursor1.close()
                conn1.close()
                cursor2.close()
                conn2.close()
                cursor3.close()
                conn3.close()

        if logstatus != 0:
            DaquLogJXIntoMysql("??????????????????", "", 1, jrsjid)
        else:
            DaquLogJXIntoMysql("??????????????????", failinfo, 0, jrsjid)
    except Exception as e:
        print("CgqxzhIntoMysql(1) ----- ")
        # title = "???????????????????????????????????????????????????????????????????????????????????????"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "??????????????????"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)


#???????????????????????????txt---???????????????????????????????????????tb_hjqxzh??????
def HjqxzhIntoMysql(filepath,jrsjid):
    try:
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
                # ???datetime???str??????2020-04-21 08:00:00?????????????????????
                zdtime = datetime.datetime.strptime(lst[5], '%Y%m%d%H%M%S')

                sql_input = "INSERT INTO tb_hjqxzh (hjqxzh_stationname,hjqxzh_province,hjqxzh_city,hjqxzh_cnty," \
                            "hjqxzh_stationidd,hjqxzh_time,hjqxzh_lat,hjqxzh_lon,hjqxzh_prsavg," \
                            "hjqxzh_dew,hjqxzh_frost,hjqxzh_ice,hjqxzh_smoke,hjqxzh_haze,hjqxzh_fldu,hjqxzh_flduotime," \
                            "hjqxzh_flsa,hjqxzh_flsaotime,hjqxzh_duwhr,hjqxzh_mist,hjqxzh_lit,hjqxzh_aur,hjqxzh_gawin," \
                            "hjqxzh_gss,hjqxzh_thund,hjqxzh_squa,hjqxzh_tord,hjqxzh_sast,hjqxzh_sastotime,hjqxzh_drsnow," \
                            "hjqxzh_snowst,hjqxzh_fog,hjqxzh_fogotime,hjqxzh_sori,hjqxzh_soriotime,hjqxzh_glaze,hjqxzh_glazeotime,hjqxzh_rain," \
                            "hjqxzh_preotime,hjqxzh_snow,hjqxzh_snowotime,hjqxzh_hail,hjqxzh_hailotime,hjqxzh_jrsjid) " \
                            "VALUES ('%s', '%s', '%s', '%s', %s, '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s,%s, %s, %s, %s, %s, %s, %s, %s)" \
                            % (lst[0], lst[1], lst[3], lst[4], lst[6], zdtime, lst[11], lst[12], lst[13], lst[14]
                               , lst[15], lst[16], lst[17], lst[18], lst[19],lst[20],lst[21],lst[22],lst[23],lst[24],
                               lst[25],lst[26],lst[27],lst[28],lst[29],lst[30],lst[31],lst[32],lst[33],lst[34],lst[35],lst[36],lst[37],lst[38],lst[39], lst[40],
                               lst[41],lst[42],lst[43],lst[44],lst[45],lst[46],lst[47],jrsjid)
                # print(sql_input)
                conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                cursor1 = conn1.cursor()
                conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
                cursor2 = conn2.cursor()
                conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
                cursor3 = conn3.cursor()

                cursor1.execute(sql_input)
                cursor2.execute(sql_input)
                cursor3.execute(sql_input)
            except Exception as e:
                print("HjqxzhIntoMysql(2) ----- ")
                logstatus = 0
                type = "??????????????????"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)
            finally:
                cursor1.close()
                conn1.close()
                cursor2.close()
                conn2.close()
                cursor3.close()
                conn3.close()

        if logstatus != 0:
            DaquLogJXIntoMysql("??????????????????", "", 1, jrsjid)
        else:
            DaquLogJXIntoMysql("??????????????????", failinfo, 0, jrsjid)
    except Exception as e:
        print("HjqxzhIntoMysql(1) ----- ")
        # print(traceback.format_exc())
        # title = "?????????????????????????????????????????????????????????????????????????????????????????????"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo+"," +str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "??????????????????"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)




#???????????????????????????txt---???????????????????????????????????????tb_dxjbzh??????
def DxjbzhIntoMysql(filepath,jrsjid):
    try:
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
                # ???datetime???str??????2020-04-21 08:00:00?????????????????????
                zdtime = datetime.datetime.strptime(lst[5], '%Y%m%d%H%M%S')

                sql_input = "INSERT INTO tb_dxjbzh (dxjbzh_stationname,dxjbzh_province,dxjbzh_city,dxjbzh_cnty," \
                            "dxjbzh_stationidd,dxjbzh_time,dxjbzh_lat,dxjbzh_lon,dxjbzh_eice,dxjbzh_eicetns," \
                            "dxjbzh_eicetwe,dxjbzh_eicewns,dxjbzh_eicewwe,dxjbzh_eicedns,dxjbzh_eicedwe,dxjbzh_jrsjid) " \
                            "VALUES ('%s', '%s', '%s', '%s', %s,'%s',%s,%s, %s, %s,%s,%s,%s,%s,%s,%s)" \
                            % (lst[0], lst[1], lst[3], lst[4], lst[6], zdtime, lst[11], lst[12], lst[13], lst[14]
                               , lst[15], lst[16], lst[17], lst[18], lst[19], jrsjid)
                conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                cursor1 = conn1.cursor()
                conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
                cursor2 = conn2.cursor()
                conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
                cursor3 = conn3.cursor()

                cursor1.execute(sql_input)
                cursor2.execute(sql_input)
                cursor3.execute(sql_input)
            except Exception as e:
                print("DxjbzhIntoMysql(2) ----- ")
                logstatus = 0
                type = "??????????????????"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)
            finally:
                cursor1.close()
                conn1.close()
                cursor2.close()
                conn2.close()
                cursor3.close()
                conn3.close()

        if logstatus != 0:
            DaquLogJXIntoMysql("??????????????????", "", 1, jrsjid)
        else:
            DaquLogJXIntoMysql("??????????????????", failinfo, 0, jrsjid)
    except Exception as e:
        print("DxjbzhIntoMysql(1) ----- ")
        # title = "?????????????????????????????????????????????????????????????????????????????????????????????"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "??????????????????"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)


#????????????????????????txt---???????????????????????????????????????tb_cgqxzh ??????
def RadiIntoMysql(filepath,jrsjid):
    try:
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
                # ???datetime???str??????2020-04-21 08:00:00?????????????????????
                # print(lst[4])
                zdtime = datetime.datetime.strptime(lst[4], '%Y%m%d%H%M%S')
                sql_input = "INSERT INTO tb_radi (radi_stationname, radi_province, radi_city, radi_cnty," \
                            "radi_stationidd, radi_time, radi_lat,radi_lon,radi_alti,radi_v14311,radi_v14320,radi_jrsjid) " \
                            "VALUES ('%s', '%s', '%s', '%s', %s,'%s',%s,%s, %s,%s, %s, %s)" \
                            % (lst[0], lst[1], lst[2], lst[3], lst[5], zdtime, lst[7], lst[8], lst[9], lst[15], lst[16],
                               jrsjid)
                
                conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                cursor1 = conn1.cursor()
                conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
                cursor2 = conn2.cursor()
                conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
                cursor3 = conn3.cursor()

                cursor1.execute(sql_input)
                cursor2.execute(sql_input)
                cursor3.execute(sql_input)
            except Exception as e:
                print("RadiIntoMysql(2) ----- ")
                logstatus = 0
                type = "??????????????????"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)
            finally:
                cursor1.close()
                conn1.close()
                cursor2.close()
                conn2.close()
                cursor3.close()
                conn3.close()

        if logstatus != 0:
            DaquLogJXIntoMysql("??????????????????", "", 1, jrsjid)
        else:
            DaquLogJXIntoMysql("??????????????????", failinfo, 0, jrsjid)
    except Exception as e:
        print("RadiIntoMysql(1) ----- ")
        # title = "??????????????????????????????????????????????????????????????????????????????????????????"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "??????????????????"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)

#OCF???????????????json????????????---???????????????????????????????????????tb_ocf ??????
def OCFIntoMysql(filepath,jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
        elif (".json" in filepath):
            file = open(filepath, 'rb')
        jsondata = json.load(file)
        ocfdata = jsondata['data']

        logstatus = 1
        failinfo = " "
        for key in ocfdata:
            try:
                # print(ocfdata[key][0]['cityinfo'])
                areaid = ocfdata[key][0]['cityinfo']['areaid'] # 9????????????
                city = ocfdata[key][0]['cityinfo']['city'] # ???
                country = ocfdata[key][0]['cityinfo']['country'] # ??????
                lat = ocfdata[key][0]['cityinfo']['lat'] # ??????
                lon = ocfdata[key][0]['cityinfo']['lon'] # ??????
                namecn = ocfdata[key][0]['cityinfo']['namecn']  # ????????????
                province = ocfdata[key][0]['cityinfo']['province'] # ??????
                time = ocfdata[key][0]['cityinfo']['time'] # ????????????
                time_zone = ocfdata[key][0]['cityinfo']['time_zone'] #????????????
                zone_abb = ocfdata[key][0]['cityinfo']['zone_abb'] #????????????
                j0 = ocfdata[key][1]['j']['j0'] #
                # print(areaid +","+city +","+country +","+lat +","+lon +","+namecn +","+province +","+time +","+time_zone +","+zone_abb +","+j0)
                j1list = ocfdata[key][1]['j']['j1']
                for item in j1list:
                    ja = item['ja'] # ??????????????????
                    jb = item['jb'] # ????????????
                    jc = item['jc']  # ????????????
                    jd = item['jd']  # 0????????????1?????????2??????
                    je = item['je']  # ???????????????
                    jf = item['jf']  # ??????
                    jg = item['jg']  # ????????????
                    jh = item['jh']  # ??????
                    ji = item['ji']  # ???????????????????????????
                    jk = item['jk']  # 360?????????
                    jl = item['jl']  # 16????????????
                    jm = item['jm']  # ????????????
                    # print(ja + "," + jb + "," + jc + "," + jd + "," + je + "," + jf + "," + jg + "," + jh + "," + ji + "," + str(jk )+ "," + jl+ "," + str(jm))

                    sql_input = "INSERT INTO tb_ocf (ocf_areaid,ocf_city,ocf_country,ocf_lat,ocf_lon,ocf_namecn,ocf_province,ocf_time,ocf_timezone,ocf_zoneabb,ocf_j0," \
                                "ocf_ja,ocf_jb,ocf_jc,ocf_jd,ocf_je,ocf_jf,ocf_jg,ocf_jh,ocf_ji,ocf_jk,ocf_jl,ocf_jm,ocf_jrsjid)"\
                                "VALUES ('%s', '%s', '%s', %s, %s,'%s','%s','%s', '%s', '%s','%s','%s','%s','%s',%s,%s,%s,%s,%s,'%s',%s,'%s',%s,%s)" \
                                % (areaid, city, country, lat, lon, namecn, province, time, time_zone, zone_abb, j0,
                                   ja,jb,jc,jd,je,jf,jg,jh,ji,jk,jl,jm,jrsjid)
                    
                    try:
                        conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                        cursor1 = conn1.cursor()
                        conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
                        cursor2 = conn2.cursor()
                        conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
                        cursor3 = conn3.cursor()

                        cursor1.execute(sql_input)
                        cursor2.execute(sql_input)
                        cursor3.execute(sql_input)
                    except Exception as e:
                        print("OCFIntoMysql(3) ----- ")
                    finally:
                        cursor1.close()
                        conn1.close()
                        cursor2.close()
                        conn2.close()
                        cursor3.close()
                        conn3.close()
            except Exception as e:
                print("OCFIntoMysql(2) ----- ")
                logstatus = 0
                type = "??????????????????"
                failinfo = ""
                for each in e.args:
                    if len(failinfo) == 0:
                        failinfo = failinfo + str(each)
                    else:
                        failinfo = failinfo + "," + str(each)
        
        if logstatus != 0:
            DaquLogJXIntoMysql("??????????????????", "", 1, jrsjid)
        else:
            DaquLogJXIntoMysql("??????????????????", failinfo, 0, jrsjid)
    except Exception as e:
        print("OCFIntoMysql(1) ----- ")
        # title = "OCF????????????????????????????????????????????????????????????????????????????????????"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertJxFailLog(jrsjid, title, failinfo)
        type = "??????????????????"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogJXIntoMysql(type, failinfo, 0, jrsjid)

def SCWJxIntoMysql(filepath,jrsjid):
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
                                startTime = startTimeStr[0:4]+'-'+startTimeStr[4:6]+'-'+startTimeStr[6:8]+' '+startTimeStr[8:10]+':'+startTimeStr[10:12]+':00'
                            if key == 'endTime':
                                endTimeStr = jsonObj[key]
                                endTime = endTimeStr[0:4]+'-'+endTimeStr[4:6]+'-'+endTimeStr[6:8]+' '+endTimeStr[8:10]+':'+endTimeStr[10:12]+':00'
                            if key == 'title':
                                title = jsonObj[key]
                            if key == 'forecast':
                                # ???????????????
                                forecast = jsonObj[key]
                        for key in jsonObj:
                            if key == 'obs':
                                # ?????????????????????
                                obs = jsonObj[key]
                                for el in obs:
                                    # ???????????????
                                    if el == 'rain':
                                        rain = obs[el]
                                        getScwLocation(rain,el,startTime,endTime,jrsjid)
                                    # ????????????
                                    if el == 'wind':
                                        wind = obs[el]
                                        getScwLocation(wind,el,startTime,endTime,jrsjid)
                                    # ??????
                                    if el == 'hail':
                                        hail = obs[el]
                                        getScwLocation(hail,el,startTime,endTime,jrsjid)
                                    # ??????????????????????????????
                                    if el == 'rain_wind':
                                        rain_wind = obs[el]
                                        getScwLocation(rain_wind,el,startTime,endTime,jrsjid)
                                    # ????????????????????????
                                    if el == 'rain_hail':
                                        rain_hail = obs[el]
                                        getScwLocation(rain_hail,el,startTime,endTime,jrsjid)
                                    # ?????????????????????
                                    if el == 'wind_hail':
                                        wind_hail = obs[el]
                                        getScwLocation(wind_hail,el,startTime,endTime,jrsjid)
                                    # ???????????????????????????????????????
                                    if el == 'rain_wind_hail':
                                        rain_wind_hail = obs[el]
                                        getScwLocation(rain_wind_hail,el,startTime,endTime,jrsjid)

            TarFile.close()
    except Exception as e:
        print("SCWJxIntoMysql(1) ----- " + str(e))


# ??????scw???????????????
def getScwLocation(data,elName,startTime,endTime,jrsjid):
    # ??????????????????
    try:
        conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
        cursor1 = conn1.cursor()
        conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
        cursor2 = conn2.cursor()
        conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
        cursor3 = conn3.cursor()

        for item in data:
            for key in item:
                # ??????????????????(????????????)
                if key == 'intensity':
                    intensity = item[key]
            for key in item:
                # ???????????????
                if key == 'xy':
                    xy = item[key]
                    lat = []
                    lon = []
                    for index in range(len(xy)):
                        # ???1???????????????
                        if index % 2 == 0:
                            lat.append(xy[index])
                        # ???2???????????????
                        else:
                            lon.append(xy[index])
                    # ???????????????mysql
                    for index in range(len(lat)):
                        sql = "INSERT INTO tb_scw (scw_lat,scw_lon,scw_type,scw_start,scw_end,scw_intensity,scw_jrsjid) VALUES ('%s','%s','%s','%s','%s','%s','%s')" % (
                            lat[index], lon[index], elName, startTime, endTime, intensity, jrsjid)
                        try:
                            cursor1.execute(sql)
                            cursor2.execute(sql)
                            cursor3.execute(sql)
                        except Exception as e:
                            print("getScwLocation(2) ----- " + str(e))
                        
    except Exception as e:
        print("getScwLocation(1) ----- " + str(e))
    finally:
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
            SkzdzsIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjSkzszsJxIntoMysql ----- ")

def JrsjSkzszrJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            SkzdzrIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjSkzszrJxIntoMysql ----- ")


def JrsjAlarmJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            # filepath = item[12].split(".")[0] + ".xml"
            filepath = item[12]
            jrsjid = item[0]
            AlarmJxIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjAlarmJxIntoMysql ----- ")


def JrsjCgqxzhJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            CgqxzhIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjCgqxzhJxIntoMysql ----- ")



def JrsjHjqxzhJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            HjqxzhIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjHjqxzhJxIntoMysql ----- ")


def JrsjDxjbzhIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            DxjbzhIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjDxjbzhIntoMysql ----- ")


def JrsjRadiJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            RadiIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjRadiJxIntoMysql ----- ")



def JrsjOCFJxIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            OCFIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjOCFJxIntoMysql ----- ")

#jrsj_jxstorestatus?????????tb_jrsj?????????????????????????????????mysql??????????????????0,0??????????????????1????????????????????????
def GetJrsjByTypeNoJxStore(qxsjtype):
    # db = pymysql.connect(host='localhost', user='root', passwd='1234', port=3306, db='dlqx_test')  # ???????????????
    # cursor = db.cursor()
    jrsj = []
    try:

        sql = "SELECT * FROM tb_jrsj WHERE jrsj_jxstorestatus = 0 and jrsj_qxtype = '%s'" % (qxsjtype)
        try:
            conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor = conn.cursor()
            cursor.execute(sql)
            jrsj = cursor.fetchall()
        except Exception as e:
            print("GetJrsjByTypeNoJxStore(2) ----- ")
        finally:
            return jrsj
            cursor.close()
            conn.close()
    except Exception as e:
        print("GetJrsjByTypeNoJxStore(1) ----- ")




#????????????????????????????????????jrsj_jxstorestatus???????????????1
#????????????????????????????????????jrsj_jxstorestatus???????????????1
def SetJrsjJxStatus1(qxsjs):
    # db = pymysql.connect(host='localhost', user='root', passwd='1234', port=3306, db='dlqx_test')  # ???????????????
    # cursor = db.cursor()
    try:
        for item in qxsjs:
            sql = "UPDATE  tb_jrsj SET jrsj_jxstorestatus = 1 WHERE jrsj_id = %s " % (item[0])
            try:
                # conn1 = pymysql.connect(host='localhost', user='root', passwd='', port=3306, db='dlqx13306')  # ???????????????
                # cursor1 = conn1.cursor()
                conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                cursor1 = conn1.cursor()
                # conn2 = pymysql.connect(host='localhost', user='root', passwd='', port=3306, db='dlqx13307')  # ???????????????
                # cursor2 = conn2.cursor()
                conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
                cursor2 = conn2.cursor()
                # conn3 = pymysql.connect(host='localhost', user='root', passwd='', port=3306, db='dlqx13308')  # ???????????????
                # cursor3 = conn3.cursor()
                conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
                cursor3 = conn3.cursor()

                cursor1.execute(sql)
                # conn1.commit()
                cursor2.execute(sql)
                # conn2.commit()
                cursor3.execute(sql)
                # conn3.commit()
            except Exception as e:
                print("SetJrsjJxStatus1(2) ----- " + str(e))
            finally:
                cursor1.close()
                conn1.close()
                cursor2.close()
                conn2.close()
                cursor3.close()
                conn3.close()
    except Exception as e:
        print("SetJrsjJxStatus1(1) ----- " + str(e))

# if __name__ == '__main__':
#     HjqxzhIntoMysql("F:/lscf/HJ_QXZH_20160129000000.tar.gz",8035)
    # jrsjgjzd = GetJrsjByTypeNoJxStore("???????????????")
    # JrsjSkzszsJxIntoMysql(jrsjgjzd)
    # jrsjzdzd = GetJrsjByTypeNoJxStore("???????????????")
    # JrsjSkzszsJxIntoMysql(jrsjzdzd)
    # jrsj = GetJrsjByTypeNoJxStore("???????????????")
    # JrsjRadiJxIntoMysql(jrsj)
    # jrsjalarm = GetJrsjByTypeNoJxStore("????????????")
    # JrsjAlarmJxIntoMysql(jrsjalarm)
    # jrsjCgqxzh = GetJrsjByTypeNoJxStore("??????????????????")
    # JrsjCgqxzhJxIntoMysql(jrsjCgqxzh)
    # jrsjHjqxzh = GetJrsjByTypeNoJxStore("??????????????????")
    # JrsjHjqxzhJxIntoMysql(jrsjHjqxzh)
    # jrsjDxjbzh = GetJrsjByTypeNoJxStore("??????????????????")
    # JrsjDxjbzhIntoMysql(jrsjDxjbzh)
    # jrsjOCF= GetJrsjByTypeNoJxStore("OCF???????????????")
    # JrsjOCFJxIntoMysql(jrsjOCF)


    # SetJrsjJxStatus1(jrsj)

