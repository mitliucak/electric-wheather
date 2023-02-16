import pymysql
import datetime
import tarfile
import zipfile
import xml.etree.ElementTree as ET
import DbutilsPool
from netCDF4 import Dataset
import jaydebeapi
import jpype

# jar_file = 'sgjdbc_4.3.18.1_20200115.jar'
# driver = 'sgcc.nds.jdbc.driver.NdsDriver'
# jdbc_url1 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqxsync_13306?appname=app_dlqxsync_13306&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# jdbc_url2 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqxsync_13307?appname=app_dlqxsync_13307&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# jdbc_url3 = 'jdbc:nds://172.20.42.5:18701,172.20.42.6:18701/v_18701_dlqx_zl?appname=app_dlqx_zl&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8'
# uandp = ["root", "shanxi_QX"]

jar_file = 'mysql-connector-java-8.0.11.jar'
driver = 'com.mysql.cj.jdbc.Driver'
jdbc_url1 = 'jdbc:mysql://121.52.212.109:13306/06dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
jdbc_url2 = 'jdbc:mysql://121.52.212.109:13307/07dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
jdbc_url3 = 'jdbc:mysql://121.52.212.109:13308/08dlqx_zl?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
uandp = ["root", "123456"]

def DaquLogIntoMysql(type,failinfo,status,jrsjid):

    selectjrsjsql = "SELECT * FROM tb_jrsj WHERE jrsj_id = %s" % (jrsjid)
    desc =" "

    try:
        conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
        cursor1 = conn1.cursor()
        # conn2 = DbutilsPool.get_db_pool2(2)
        # cursor2 = conn2.cursor()
        cursor1.execute(selectjrsjsql)
        jrsj = cursor1.fetchone()

        if jrsj[13] == "灾害预警":
            if status == 1:
                desc = "灾害预警接口文件"+jrsj[3]+"存储到tb_alarmfile"+"成功"
            else:
                desc = "灾害预警接口文件" + jrsj[3] + "存储到tb_alarmfile" + "失败"
        elif jrsj[13] == "国家站小时":
            if status == 1:
                desc = "国家站小时接口文件"+jrsj[3]+"存储到tb_gjzhourfile"+"成功"
            else:
                desc = "国家站小时接口文件" + jrsj[3] + "存储到tb_gjzhourfile" + "失败"
        elif jrsj[13] == "自动站小时":
            if status == 1:
                desc = "自动站小时接口文件"+jrsj[3]+"存储到tb_zdhourfile"+"成功"
            else:
                desc = "自动站小时接口文件" + jrsj[3] + "存储到tb_zdzhourfile" + "失败"
        elif jrsj[13] == "SCW强对流预报":
            if status == 1:
                desc = "SCW强对流预报接口文件"+jrsj[3]+"存储到tb_scwfile"+"成功"
            else:
                desc = "SCW强对流预报接口文件" + jrsj[3] + "存储到tb_scwfile" + "失败"
        elif jrsj[13] == "辐射逐小时":
            if status == 1:
                desc = "辐射逐小时接口文件"+jrsj[3]+"存储到tb_radifile"+"成功"
            else:
                desc = "辐射逐小时接口文件" + jrsj[3] + "存储到tb_radifile" + "失败"
        elif jrsj[13] == "OCF逐小时预报":
            if status == 1:
                desc = "OCF逐小时预报接口文件"+jrsj[3]+"存储到tb_ocffile"+"成功"
            else:
                desc = "OCF逐小时预报接口文件" + jrsj[3] + "存储到tb_ocffile" + "失败"
        elif jrsj[13] == "常规气象灾害":
            if status == 1:
                desc = "常规气象灾害接口文件"+jrsj[3]+"存储到tb_cgqxzhfile"+"成功"
            else:
                desc = "常规气象灾害接口文件" + jrsj[3] + "存储到tb_cgqxzhfile" + "失败"
        elif jrsj[13] == "环境气象灾害":
            if status == 1:
                desc = "环境气象灾害接口文件"+jrsj[3]+"存储到tb_hjqxzhfile"+"成功"
            else:
                desc = "环境气象灾害接口文件" + jrsj[3] + "存储到tb_hjqxzhfile" + "失败"
        elif jrsj[13] == "电线积冰灾害":
            if status == 1:
                desc = "电线积冰灾害接口文件"+jrsj[3]+"存储到tb_dxjbzhfile"+"成功"
            else:
                desc = "环境气象灾害接口文件" + jrsj[3] + "存储到tb_dzjbzhfile" + "失败"

        logtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # insertlogsql = "INSERT INTO tb_daqulog(daqulog_time,daqulog_type,daqulog_failinfo,daqulog_desc,daqulog_status,daqulog_jrsjid) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",%s,%s)" % (logtime, type, failinfo, desc,status,jrsjid)
        server = "0"
        insertlogsql = "INSERT INTO tb_synclog(synclog_time,synclog_type,synclog_desc,synclog_status,synclog_server) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")" %  (logtime, type , desc,status,server)
        try:
            conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
            cursor3 = conn3.cursor()
            cursor3.execute(insertlogsql)
        except Exception as e:
            print("DaquLogIntoMysql(2) ----- ")
        finally:
            cursor3.close()
            conn3.close()
    except Exception as e:
        print("DaquLogIntoMysql(1) ----- ")
    finally:
        cursor1.close()
        conn1.close()

def InsertFileStoreIntoMysqlLog(jrsjid,title,failinfo):
    try:
        conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
        cursor1 = conn1.cursor()
        conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
        cursor2 = conn2.cursor()
        conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
        cursor3 = conn3.cursor()

        selectjrsjsql = "SELECT * FROM tb_jrsj WHERE jrsj_id = %s" % (jrsjid)
        failtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            cursor1.execute(selectjrsjsql)
            jrsj = cursor1.fetchone()
            fileinfo = "文件名："+jrsj[3]+";文件接入时间："+jrsj[10].strftime('%Y-%m-%d %H:%M:%S')
            insertlogsql = "INSERT INTO tb_filestorelog(filestorelog_failtime,filestorelog_title,filestorelog_failinfo,filestorelog_fileinfo) VALUES (\"%s\",\"%s\",\"%s\",\"%s\")" % (
            failtime, title, failinfo, fileinfo)
            cursor1.execute(insertlogsql)

            cursor2.execute(insertlogsql)

            cursor3.execute(insertlogsql)
        except Exception as e:
            print("InsertFileStoreIntoMysqlLog(2) ----- ")
    except Exception as e:
        print("InsertFileStoreIntoMysqlLog(1) ----- ")
    finally:
        cursor1.close()
        cursor2.close()
        cursor3.close()
        conn1.close()
        conn2.close()
        conn3.close()

def AlarmIntoMysql(ZipFilePath,jrsjid):
    try:
        zipobj = zipfile.ZipFile(ZipFilePath, 'r')
        # zip压缩包下面可能有多个文件
        xmlname = ''
        for nameitem in zipobj.namelist():
            if 'xml' in nameitem:
                xmlname = nameitem
        fileContent = zipobj.read(xmlname).decode("utf-8")
        # print(xmlname)
        # print(fileContent)
        status = 1

        sql = "INSERT INTO tb_alarmfile (alarmfile_filename,alarmfile_content,alarmfile_jrsjid) VALUES ('%s', '%s',%s)" % (
        xmlname, fileContent, jrsjid)

        try:
            conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor = conn.cursor()
            cursor.execute(sql)
        except Exception as e:
            print("AlarmIntoMysql(2) ----- ")
        finally:
            cursor.close()
            conn.close()
            DaquLogIntoMysql("非结构化数据存储", "", 1, jrsjid)
    except Exception as e:
        print("AlarmIntoMysql(1) ----- ")
        # title = "灾害预警文件存储到tb_alarmfile失败"
        type = "非结构化数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertFileStoreIntoMysqlLog(jrsjid, title, failinfo)
        DaquLogIntoMysql(type,failinfo,0,jrsjid)




def GjzHourIntoMysql(filepath,jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
            filenames = TarFile.getnames()
            fileContent = file.read().decode("utf-8")

            for item in filenames:
                if ".TXT" in item:
                    file = (TarFile.extractfile(item))
                    filename = item
                    sql = "INSERT INTO tb_gjzhourfile (gjzhourfile_filename,gjzhourfile_content,gjzhourfile_jrsjid) VALUES ('%s', '%s',%s)" % (filename, fileContent, jrsjid)
                    try:
                        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                        cursor = conn.cursor()
                        cursor.execute(sql)
                    except Exception as e:
                        print("GjzHourIntoMysql(2) ----- ")
                    finally:
                        cursor.close()
                        conn.close()
            TarFile.close()
        DaquLogIntoMysql("非结构化数据存储", "", 1, jrsjid)
    except Exception as e:
        print("GjzHourIntoMysql(1) ----- ")
        # title = "国家站小时文件存储到tb_gjzhourfile失败"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertFileStoreIntoMysqlLog(jrsjid, title, failinfo)
        type = "非结构化数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogIntoMysql(type, failinfo, 0, jrsjid)


def ZdzHourIntoMysql(filepath,jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
            filenames = TarFile.getnames()
            fileContent = file.read().decode("utf-8")

            for item in filenames:
                if ".TXT" in item:
                    file = (TarFile.extractfile(item))
                    filename = item
                    sql = "INSERT INTO tb_zdzhourfile (zdzhourfile_filename,zdzhourfile_content,zdzhourfile_jrsjid) VALUES ('%s', '%s',%s)" % (
                    filename, fileContent, jrsjid)
                    try:
                        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                        cursor = conn.cursor()
                        cursor.execute(sql)
                    except Exception as e:
                        print("ZdzHourIntoMysql(2) ----- ")
                    finally:
                        cursor.close()
                        conn.close()
            TarFile.close()
            DaquLogIntoMysql("非结构化数据存储", "", 1, jrsjid)
    except Exception as e:
        print("ZdzHourIntoMysql(1) ----- ")
        # title = "自动站小时文件存储到tb_zdzhourfile失败"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertFileStoreIntoMysqlLog(jrsjid, title, failinfo)
        type = "非结构化数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogIntoMysql(type, failinfo, 0, jrsjid)


def SCWIntoMysql(filepath,jrsjid):
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
                    sql = "INSERT INTO tb_scwfile (scwfile_filename,scwfile_content,scwfile_jrsjid) VALUES ('%s', '%s',%s)" % (
                    filename, fileContent, jrsjid)
                    try:
                        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                        cursor = conn.cursor()
                        cursor.execute(sql)
                    except Exception as e:
                        print("SCWIntoMysql(2) ----- ")
                    finally:
                        cursor.close()
                        conn.close()
            TarFile.close()
            DaquLogIntoMysql("非结构化数据存储", "", 1, jrsjid)
    except Exception as e:
        print("SCWIntoMysql(1) ----- ")
        # title = "SCW强对流文件存储到tb_scwfile失败"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertFileStoreIntoMysqlLog(jrsjid, title, failinfo)
        type = "非结构化数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogIntoMysql(type, failinfo, 0, jrsjid)


def RadiIntoMysql(filepath,jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
            filenames = TarFile.getnames()
            fileContent = file.read().decode("utf-8")
            for item in filenames:
                if ".TXT" in item:
                    file = (TarFile.extractfile(item))
                    filename = item
                    sql = "INSERT INTO tb_radifile (radifile_filename,radifile_content,radifile_jrsjid) VALUES ('%s', '%s',%s)" % (
                    filename, fileContent, jrsjid)
                    try:
                        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                        cursor = conn.cursor()
                        cursor.execute(sql)
                    except Exception as e:
                        print("RadiIntoMysql(3) ----- ")
                    finally:
                        cursor.close()
                        conn.close()
            TarFile.close()
        elif ".TXT" in filepath:
            file = open(filepath, 'rb')
            filename = filepath.split("/")[-1]
            fileContent = file.read().decode("utf-8")
            sql = "INSERT INTO tb_radifile (radifile_filename,radifile_content,radifile_jrsjid) VALUES ('%s', '%s',%s)" % (
                filename, fileContent, jrsjid)
            try:
                conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                cursor = conn.cursor()
                cursor.execute(sql)
            except Exception as e:
                print("RadiIntoMysql(2) ----- ")
            finally:
                cursor.close()
                conn.close()
        DaquLogIntoMysql("非结构化数据存储", "", 1, jrsjid)
    except Exception as e:
        print("RadiIntoMysql(1) ----- ")
        # title = "辐射逐小时文件存储到tb_radifile失败"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertFileStoreIntoMysqlLog(jrsjid, title, failinfo)
        type = "非结构化数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogIntoMysql(type, failinfo, 0, jrsjid)


def OcfIntoMysql(filepath,jrsjid):
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
                    sql = "INSERT INTO tb_ocffile (ocffile_filename,ocffile_content,ocffile_jrsjid) VALUES ('%s', '%s',%s)" % (
                    filename, fileContent, jrsjid)
                    try:
                        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                        cursor = conn.cursor()
                        cursor.execute(sql)
                    except Exception as e:
                        print("OcfIntoMysql(2) ----- ")
                    finally:
                        cursor.close()
                        conn.close()
            TarFile.close()
            DaquLogIntoMysql("非结构化数据存储", "", 1, jrsjid)
    except Exception as e:
        print("OcfIntoMysql(1) ----- ")
        # title = "OCF文件存储到tb_ocffile失败"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertFileStoreIntoMysqlLog(jrsjid, title, failinfo)
        type = "非结构化数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogIntoMysql(type, failinfo, 0, jrsjid)


def CgqxzhIntoMysql(filepath,jrsjid):
    try:
        if ".tar.gz" in filepath:
            # print(filepath)
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
            filenames = TarFile.getnames()
            fileContent = file.read().decode("utf-8")

            for item in filenames:
                if ".TXT" in item:
                    file = (TarFile.extractfile(item))
                    filename = item
                    sql = "INSERT INTO tb_cgqxzhfile (cgqxzhfile_filename,cgqxzhfile_content,cgqxzhfile_jrsjid) VALUES ('%s', '%s',%s)" % (
                    filename, fileContent, jrsjid)
                    try:
                        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                        cursor = conn.cursor()
                        cursor.execute(sql)
                    except Exception as e:
                        print("CgqxzhIntoMysql(2) ----- ")
                    finally:
                        cursor.close()
                        conn.close()
            TarFile.close()
            DaquLogIntoMysql("非结构化数据存储", "", 1, jrsjid)
    except Exception as e:
        print("CgqxzhIntoMysql(1) ----- ")
        # title = "常规气象灾害文件存储到tb_cgqxzhfile失败"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertFileStoreIntoMysqlLog(jrsjid, title, failinfo)
        type = "非结构化数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogIntoMysql(type, failinfo, 0, jrsjid)


def HjqxzhIntoMysql(filepath,jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
            filenames = TarFile.getnames()
            fileContent = file.read().decode("utf-8")

            for item in filenames:
                if ".TXT" in item:
                    file = (TarFile.extractfile(item))
                    filename = item
                    sql = 'INSERT INTO tb_hjqxzhfile (hjqxzhfile_filename,hjqxzhfile_content,hjqxzhfile_jrsjid) VALUES ("%s", "%s",%s)' % (filename, fileContent, jrsjid)
                    try:
                        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                        cursor = conn.cursor()
                        cursor.execute(sql)
                    except Exception as e:
                        print("HjqxzhIntoMysql(2) ----- ")
                    finally:
                        cursor.close()
                        conn.close()
            TarFile.close()
            DaquLogIntoMysql("非结构化数据存储", "", 1, jrsjid)
    except Exception as e:
        print("HjqxzhIntoMysql(1) ----- ")
        # title = "环境气象灾害文件存储到tb_hjqxzhfile失败"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertFileStoreIntoMysqlLog(jrsjid, title, failinfo)
        type = "非结构化数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogIntoMysql(type, failinfo, 0, jrsjid)





def DxjbzhIntoMysql(filepath,jrsjid):
    try:
        if ".tar.gz" in filepath:
            TarFile = tarfile.open(filepath, 'r')
            file = (TarFile.extractfile(TarFile.firstmember))
            filenames = TarFile.getnames()
            fileContent = file.read().decode("utf-8")

            for item in filenames:
                if ".TXT" in item:
                    file = (TarFile.extractfile(item))
                    filename = item
                    sql = "INSERT INTO tb_dxjbzhfile (dxjbzhfile_filename,dxjbzhfile_content,dxjbzhfile_jrsjid) VALUES ('%s', '%s',%s)" % (
                    filename, fileContent, jrsjid)
                    try:
                        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                        cursor = conn.cursor()
                        cursor.execute(sql)
                    except Exception as e:
                        print("DxjbzhIntoMysql(2) ----- ")
                    finally:
                        cursor.close()
                        conn.close()
            TarFile.close()
            DaquLogIntoMysql("非结构化数据存储", "", 1, jrsjid)
    except Exception as e:
        print("DxjbzhIntoMysql(1) ----- ")
        # title = "电线积冰灾害文件存储到tb_dxjbzhfile失败"
        # failinfo = ""
        # for each in e.args:
        #     if len(failinfo) == 0:
        #         failinfo = failinfo + str(each)
        #     else:
        #         failinfo = failinfo + "," + str(each)
        # # print(failinfo)
        # InsertFileStoreIntoMysqlLog(jrsjid, title, failinfo)
        type = "非结构化数据存储"
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
        DaquLogIntoMysql(type, failinfo, 0, jrsjid)




#预报网格nc文件存储进入mysql
def YbwgNCIntoMysql(filepath,filename,jrsjid):
    try:
        nc_obj = Dataset(filepath)
        # 纬度
        lat = (nc_obj.variables['lat'][:])
        # 经度
        lon = (nc_obj.variables['lon'][:])
        # 时间（）
        time = (nc_obj.variables['time'][:])
        # 需要解析的属性变量在下标为0的位置,属性代表24小时降水，温度等等
        listKeys = list(nc_obj.variables.keys())
        Attr = (nc_obj.variables[listKeys[0]])
        # print(str(nc_obj))
        # print(type(Attr))
        attrjsondictList = []
        attrlist = []
        TimeDimension = Attr.shape[0]
        LatDimension = Attr.shape[1]
        LonDimension = Attr.shape[2]
        for i in range(TimeDimension):
            for j in range(LatDimension):
                for k in range(LonDimension):
                    # timev = time[i]
                    # latv = lat[j]
                    # lonv = lon[k]
                    attrv = Attr[i, j, k]
                    attrlist.append(round(float(attrv), 2))
                    # NcOneObjectDict = dict()
                    # NcOneObjectDict['time'] = timev
                    # NcOneObjectDict['lat'] = round(float(latv), 2)
                    # NcOneObjectDict['lon'] = round(float(lonv), 2)
                    # NcOneObjectDict[listKeys[0]] = round(float(attrv), 2)
                    # attrjsondictList.append(NcOneObjectDict)
        ncdatadict = dict()
        ncdatadict['info'] = str(nc_obj)
        ncdatadict['time'] = time.tolist()
        ncdatadict['lat'] = lat.tolist()
        ncdatadict['lon'] = lon.tolist()
        ncdatadict[listKeys[0]] = attrlist
        # print(ncdatadict)

        sql = "INSERT INTO tb_ybwgfile (ybwgfile_filename,ybwgfile_content,ybwgfile_jrsjid) VALUES ('%s', '%s',%s)" % (
            filename, pymysql.escape_string(str(ncdatadict)), jrsjid)
        try:
            conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor = conn.cursor()
            cursor.execute(sql)
        except Exception as e:
            print("YbwgNCIntoMysql(2) ----- ")
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print("YbwgNCIntoMysql(1) ----- ")


# 实时网格nc文件---3KM网格存储进入mysql
def SswgNCIntoMysql(filepath, filename, jrsjid):
    # try:
        nc_obj = Dataset(filepath)
        # print(nc_obj)
        x = (nc_obj.variables['x'][:])
        y = (nc_obj.variables['y'][:])
        # 露点温度
        Dewpoint_temperature_height_above_ground = (nc_obj.variables['Dewpoint_temperature_height_above_ground'][:])
        # 相对湿度
        Relative_humidity_height_above_ground = (nc_obj.variables['Relative_humidity_height_above_ground'][:])
        # 温度
        Temperature_height_above_ground = (nc_obj.variables['Temperature_height_above_ground'][:])
        # 1小时累计降水量
        Total_precipitation_surface = (nc_obj.variables['Total_precipitation_surface'][:])
        # 2分钟平均风向
        Wind_direction_from_which_blowing_height_above_ground = (
            nc_obj.variables['Wind_direction_from_which_blowing_height_above_ground'][:])
        # 2分钟平均风速
        Wind_speed_height_above_ground = (nc_obj.variables['Wind_speed_height_above_ground'][:])
        # U风分量
        u_component_of_wind_height_above_ground = (nc_obj.variables['u-component_of_wind_height_above_ground'][:])
        # v风分量
        v_component_of_wind_height_above_ground = (nc_obj.variables['v-component_of_wind_height_above_ground'][:])

        XDimension = Dewpoint_temperature_height_above_ground.shape[3]
        YDimension = Dewpoint_temperature_height_above_ground.shape[2]
        # print(YDimension)
        # print(XDimension)
        LDTemList = []
        XdsdList = []
        TemList = []
        JslYxsList = []
        WindDirectList = []
        WindSpeedList = []
        UComList = []
        VComList = []
        for i in range(YDimension):
            for j in range(XDimension):
                # 露点温度
                LDTem = Dewpoint_temperature_height_above_ground[0, 0, i, j]
                # 相对湿度
                Xdsd = Relative_humidity_height_above_ground[0, 0, i, j]
                # 温度
                Tem = Temperature_height_above_ground[0, 0, i, j]
                # 1小时累计降水量
                JslYxs = Total_precipitation_surface[0, i, j]
                # 2分钟平均风向
                WindDirect = Wind_direction_from_which_blowing_height_above_ground[0, 0, i, j]
                # 2分钟平均风速
                WindSpeed = Wind_speed_height_above_ground[0, 0, i, j]
                # U风分量
                UCom = u_component_of_wind_height_above_ground[0, 0, i, j]
                # v风分量
                VCom = v_component_of_wind_height_above_ground[0, 0, i, j]

                LDTemList.append(round(float(LDTem), 2))
                XdsdList.append(round(float(Xdsd), 2))
                TemList.append(round(float(Tem), 2))
                JslYxsList.append(round(float(JslYxs), 2))
                WindDirectList.append(round(float(WindDirect), 2))
                WindSpeedList.append(round(float(WindSpeed), 2))
                UComList.append(round(float(UCom), 2))
                VComList.append(round(float(VCom), 2))

        ncdatadict = dict()
        ncdatadict['info'] = str(nc_obj)
        ncdatadict['y'] = y.tolist()
        ncdatadict['x'] = x.tolist()
        ncdatadict['Dewpoint_temperature_height_above_ground'] = LDTemList
        ncdatadict['Relative_humidity_height_above_ground'] = XdsdList
        ncdatadict['Temperature_height_above_ground'] = TemList
        ncdatadict['Total_precipitation_surface'] = JslYxsList
        ncdatadict['Wind_direction_from_which_blowing_height_above_ground'] = WindDirectList
        ncdatadict['Wind_speed_height_above_ground'] = WindSpeedList
        ncdatadict['u_component_of_wind_height_above_ground'] = UComList
        ncdatadict['v_component_of_wind_height_above_ground'] = VComList
        
        sql = "INSERT INTO tb_sswgfile (sswgfile_filename,sswgfile_content,sswgfile_jrsjid) VALUES ('%s', '%s',%s)" % (
            filename, pymysql.escape_string(str(ncdatadict)), jrsjid)
        try:
            conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor = conn.cursor()
            cursor.execute(sql)
        except Exception as e:
            print("SswgNCIntoMysql ----- ")
        finally:
            cursor.close()
            conn.close()
    # except Exception as e:
    #     print(e)

def JrsjAlarmFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            AlarmIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjAlarmFilesIntoMysql ----- ")

def JrsjGjzHourFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            GjzHourIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjGjzHourFilesIntoMysql ----- ")

def JrsjZdzHourFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            ZdzHourIntoMysql(filepath, jrsjid)
    except Exception as e:
        print("JrsjZdzHourFilesIntoMysql ----- ")


def JrsjSCWHourFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            SCWIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjSCWHourFilesIntoMysql ----- ")

def JrsjRadiHourFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            RadiIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjRadiHourFilesIntoMysql ----- ")

def JrsjOcfHourFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            OcfIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjOcfHourFilesIntoMysql ----- ")

def JrsjCgqxzhFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            CgqxzhIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjCgqxzhFilesIntoMysql ----- ")

def JrsjHjqxzhFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            HjqxzhIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjHjqxzhFilesIntoMysql ----- ")

def JrsjDxjbzhFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            jrsjid = item[0]
            DxjbzhIntoMysql(filepath,jrsjid)
    except Exception as e:
        print("JrsjDxjbzhFilesIntoMysql ----- ")

def JrsjYbNCFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            filename = item[3]
            jrsjid = item[0]
            YbwgNCIntoMysql(filepath,filename,jrsjid)
    except Exception as e:
        print("JrsjYbNCFilesIntoMysql ----- ")

def JrsjSsNCFilesIntoMysql(jrsj):
    try:
        for item in jrsj:
            filepath = item[12]
            filename = item[3]
            jrsjid = item[0]
            SswgNCIntoMysql(filepath,filename,jrsjid)
    except Exception as e:
        print("JrsjSsNCFilesIntoMysql ----- ")

#jrsj_filestorestatus：标志tb_jrsj接口数据是源文件的内容是否存储到了mysql表中，默认为0,0标志未同步，1标志该项已经同步
def GetJrsjByTypeNoFileStore(qxsjtype):
    print("qxsjtype" + str(qxsjtype))
    print("def GetJrsjByTypeNoFileStore(qxsjtype):")
    # db = pymysql.connect(host='localhost', user='root', passwd='1234', port=3306, db='dlqx_test')  # 连接数据库
    # cursor = db.cursor()
    jrsj = []
    try:
        sql = "SELECT * FROM tb_jrsj WHERE jrsj_filestorestatus = 0 and jrsj_qxtype = '%s'" % (qxsjtype)
        try:
            conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor = conn.cursor()
            cursor.execute(sql)
            jrsj = cursor.fetchall()
        except Exception as e:
            print("GetJrsjByTypeNoFileStore(2) ----- ")
        finally:
            return jrsj
            cursor.close()
            conn.close()
    except Exception as e:
        print("GetJrsjByTypeNoFileStore(1) ----- ")

#数据存储成功之后使用：将jrsj_filestorestatus标志未改为1
def SetJrsjFileStoreStatus1(qxsjs):
    # db = pymysql.connect(host='localhost', user='root', passwd='1234', port=3306, db='dlqx_test')  # 连接数据库
    # cursor = db.cursor()
    try:
        for item in qxsjs:
            sql = "UPDATE  tb_jrsj SET jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (item[0])
            try:
                conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
                cursor = conn.cursor()
                cursor.execute(sql)
            except Exception as e:
                print("SetJrsjFileStoreStatus1(2) ----- ")
            finally:
                cursor.close()
                conn.close()
    except Exception as e:
        print("SetJrsjFileStoreStatus1(1) ----- ")


# if __name__ == '__main__':
#     RadiIntoMysql("F:/lscf/SX_RADI_HOR_20200809010000.TXT",15293)
#     # ncjrsj = GetJrsjByTypeNoFileStore("5KM预报网格")
#     # JrsjYbNCFilesIntoMysql(ncjrsj)
#     ncssjrsj = GetJrsjByTypeNoFileStore("3KM实时网格")
#     JrsjSsNCFilesIntoMysql(ncssjrsj)
    # jrsjCgqxzh = GetJrsjByTypeNoFileStore("常规气象灾害")
    # jrsjHjqxzh = GetJrsjByTypeNoFileStore("环境气象灾害")
    # jrsjDxjbzh = GetJrsjByTypeNoFileStore("电线积冰灾害")
    # jrsjAlarm = GetJrsjByTypeNoFileStore("灾害预警")
    # jrsjGjzHour = GetJrsjByTypeNoFileStore("国家站小时")
    # jrsjZdzHour = GetJrsjByTypeNoFileStore("自动站小时")
    # jrsjSCW = GetJrsjByTypeNoFileStore("SCW强对流预报")
    # jrsjOCF= GetJrsjByTypeNoFileStore("OCF逐小时预报")
    # jrsjRadi = GetJrsjByTypeNoFileStore("辐射逐小时")
    # if len(jrsjCgqxzh) != 0:
    #     JrsjCgqxzhFilesIntoMysql(jrsjCgqxzh)
    #     SetJrsjFileStoreStatus1(jrsjCgqxzh)
    # if len(jrsjHjqxzh) != 0:
    #     JrsjHjqxzhFilesIntoMysql(jrsjHjqxzh)
    #     SetJrsjFileStoreStatus1(jrsjHjqxzh)
    # if len(jrsjDxjbzh) != 0:
    #     JrsjDxjbzhFilesIntoMysql(jrsjDxjbzh)
    #     SetJrsjFileStoreStatus1(jrsjDxjbzh)
    # if len(jrsjAlarm) != 0:
    #     JrsjAlarmFilesIntoMysql(jrsjAlarm)
    #     SetJrsjFileStoreStatus1(jrsjAlarm)
    # if len(jrsjGjzHour) != 0:
    #     JrsjGjzHourFilesIntoMysql(jrsjGjzHour)
        # SetJrsjFileStoreStatus1(jrsjGjzHour)
    # if len(jrsjZdzHour) != 0:
    #     JrsjZdzHourFilesIntoMysql(jrsjZdzHour)
    #     SetJrsjFileStoreStatus1(jrsjZdzHour)
    # if len(jrsjSCW) != 0:
    #     JrsjSCWHourFilesIntoMysql(jrsjSCW)
        # SetJrsjFileStoreStatus1(jrsjZdzHour)
    # if len(jrsjOCF) != 0:
    #     JrsjOcfHourFilesIntoMysql(jrsjOCF)
    #     # SetJrsjFileStoreStatus1(jrsjZdzHour)
    # if len(jrsjRadi) != 0:
    #     JrsjRadiHourFilesIntoMysql(jrsjRadi)
    #     # SetJrsjFileStoreStatus1(jrsjZdzHour)