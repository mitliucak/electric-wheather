import requests
import re
import pymysql
import time
import datetime
import os
import zipfile
import tarfile
import DbutilsPool
from bs4 import BeautifulSoup
import configparser
import ConfigParam
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

def DaquLogJrsjIntoMysql(type,failinfo,status,url, name, dataType, size, update):
    print("接入数据下载调用DaquLogJrsjIntoMysql")
    jkinfo = {"url": url,
              "name": name,
              "datatype": dataType,
              "size": size,
              "update": update}
    if status == 1:
        desc = "接入数据" + name + "下载成功, 接口信息:" + str(jkinfo)
        print(desc)
    else:
        desc = "接入数据" + name + "下载失败, 接口信息:" + str(jkinfo)
        print(desc)
    logtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insertlogsql = "INSERT INTO tb_daqulog(daqulog_time,daqulog_type,daqulog_failinfo,daqulog_desc,daqulog_status) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",%s)" % (logtime, type, failinfo, desc,status)
    try:
        conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
        cursor3 = conn3.cursor()
        cursor3.execute(insertlogsql)
    except Exception as e:
        print("DaquLogJrsjIntoMysql ----- "  + str(e))
    finally:
        cursor3.close()
        conn3.close()

def getHTMLText(url):
    try:
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        r = requests.get(url, headers=headers,timeout = 30)
        r.encoding = 'utf-8'
        r.raise_for_status()
        return r.text
    except Exception as e:
        print("getHTMLText ----- " + str(e))

#已经无效--最开始是先解压然后读取文件，后期直接从压缩包中读取文件内容
def unzip():
    # db = pymysql.connect(host='localhost', user='root', passwd='1234', port=3306, db='dlqx_test')  # 连接数据库
    # cursor = db.cursor()
    sql = 'SELECT id, qxsj_path FROM tb_qxsj'
    try:
        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
        cursor = conn.cursor()
        cursor.execute(sql)
        qxsj = cursor.fetchall()
        for one in qxsj:
            filepath = one[1].replace("\\", "/")
            dest_dir = filepath.rsplit("/", 1)[0]
            if ".tar" in filepath:
                tar = tarfile.open(filepath,'r')
                tarnames = tar.getnames()
                for item in tarnames:
                    tar.extract(item, dest_dir)
                tar.close()
            if ".zip" in filepath:
                zip_file = zipfile.ZipFile(filepath)
                for names in zip_file.namelist():
                    zip_file.extract(names, dest_dir)
                zip_file.close()
    except Exception as e:
        print("unzip ----- "  + str(e))
    finally:
        cursor.close()
        conn.close()

 #根据文件名判断该接口是否已经写入mysql
def FileExist(filename,update):
    try:
        # db = pymysql.connect(host='localhost', user='root', passwd='1234', port=3306, db='dlqx_test')  # 连接数据库
        # cursor = db.cursor()
        conn = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
        cursor = conn.cursor()
        sql = "SELECT * FROM tb_qxsj WHERE  qxsj_name = '%s' AND qxsj_update = '%s'" %(filename, update)
        cursor.execute(sql)
        qxsj = cursor.fetchall()
    except Exception as e:
        print("FileExist(2) ----- "  + str(e))
        cursor.close()
        conn.close()
        return 0
    finally:
        cursor.close()
        conn.close()
        if len(qxsj) > 0:
            return 1
        else:
            return 0

def saveLink(path, name, dataType, size, update):
    # print(path, name, dataType, size, update)
    # 当前目录
    current_path = os.getcwd()
    # print(current_path)
    # 下载文件
    print("调用saveLink下载文件")
    Download_addres = path
    file = requests.get(Download_addres)
    with open(name, "wb") as code:
        code.write(file.content)
        file_path1 = current_path + "/" + name
        # file_path = file_path1.strip().replace('\\','\\\\')
        #Linux 下文件目录 '/' ,windows默认 '\', windows'/' 也可以识别无空格即可
        file_path = file_path1.strip().replace('\\', '/')
        #文件下载结束后，若文件名中有.tar 或者.zip 进行解压
        print("文件下载结束后，若文件名中有.tar 或者.zip 进行解压")
        # #=============================连接数据库==================================
        # db = pymysql.connect(host='localhost',user='root',passwd='1234',port=3306,db='dlqx_test' ) #连接数据库
        # cursor = db.cursor()
        # 结构化数据写入第1个mysql
        # print(dataType)
        sql = "INSERT INTO tb_qxsj(qxsj_url, qxsj_name, qxsj_type, qxsj_size, qxsj_update, qxsj_path) \
           VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % (path, name, dataType, size, update, file_path)
        try:
            conn1 = jaydebeapi.connect(driver,jdbc_url1,uandp,jar_file)
            cursor1 = conn1.cursor()
            cursor1.execute(sql)
        except Exception as e:
            print("saveLink(1) ----- " + str(e))
        finally:
            cursor1.close()
            conn1.close()
        #结构化数据写入第2个mysql
        try:
            conn2 = jaydebeapi.connect(driver,jdbc_url2,uandp,jar_file)
            cursor2 = conn2.cursor()
            cursor2.execute(sql)
        except Exception as e:
            print("saveLink(2) ----- " + str(e))
        finally:
            cursor2.close()
            conn2.close()
        # 结构化数据写入第3个mysql
        try:
            conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
            cursor3 = conn3.cursor()
            cursor3.execute(sql)
        except Exception as e:
            print("saveLink(3) ----- " + str(e))
        finally:
            cursor3.close()
            conn3.close()

def getUrl(url, name, dataType, size, update):
    print("调用getUrl做nc压缩文件下载处理")
    try:
        html = getHTMLText(url)
        soup = BeautifulSoup(html,"lxml")
        data = soup.select("ol li")
        # print(data)
        for i in data:
            # print("i" + str(i))
            if i.select("b")[0].get_text().strip() == "HTTPServer:":
                print("走进if的判断")
                # print("i" + str(i))
                link = i.select("a")[0].get('href',None)
                path = url.split("/thredds")[0] + link
                # saveLink(path, name, dataType, size, update)
                #若该接口以前没有写入mysql则写入，如果曾经写入过则不重新写入避免数据重复
                try:
                    print("name"+name+"update"+update)
                    if FileExist(name,update) == 0:
                        print("若该接口以前没有写入mysql则写入，如果曾经写入过则不重新写入避免数据重复")
                        saveLink(path, name, dataType, size, update)
                except Exception as e:
                    print("getUrl(1) ----- " + str(e))
        DaquLogJrsjIntoMysql("接入数据下载", " ", 1, url, name, dataType, size, update)
    except Exception as e:
        failinfo = ""
        for each in e.args:
            if len(failinfo) == 0:
                failinfo = failinfo + str(each)
            else:
                failinfo = failinfo + "," + str(each)
            print(failinfo)
        DaquLogJrsjIntoMysql("接入数据下载", failinfo, 0, url, name, dataType, size, update)

def getlist(url, dataType):
    print("url+++" + str(url))
    print("进入getlist方法准备抓取")
    try:
        html = getHTMLText(url)
        soup = BeautifulSoup(html,"lxml")
        data = soup.select("tr")
        count = 0
        if dataType == "灾害预警":
            print("进入循环灾害预警")
            for i in data:
                if len(i.select("td")) > 0:
                    print("进入第一个if")
                    name = i.select("td")[0].select("tt")[0].get_text().strip()
                    if ".tar.gz" in name or ".zip" in name or ".TXT" in name or ".nc" in name:
                        print("进入第二个if")
                        link = i.select("td")[0].select("a")[0].get('href',None)
                        path = url.split("/catalog.html")[0] + "/" + link
                        size = i.select("td")[1].select("tt")[0].get_text().strip()
                        update = i.select("td")[2].select("tt")[0].get_text().strip()
                        getUrl(path, name, dataType, size, update)
                    elif "/" in name:
                        print("进入否则")
                        link = i.select("td")[0].select("a")[0].get('href',None)
                        path = url.split("/catalog.html")[0] + "/" + link
                        getlist(path, dataType)
        else:
            for i in data:
                print("for循环做判断")
                if count > 10:
                    print(count)
                    break
                else:
                    if len(i.select("td")) > 0:
                        print("进入else")
                        name = i.select("td")[0].select("tt")[0].get_text().strip()
                        print("打印name"+name)
                        if ".tar.gz" in name or ".zip" in name or ".TXT" in name or ".nc" in name:
                            print(".tar.gz in name or .zip in name or .TXT in name or .nc in name:")
                            link = i.select("td")[0].select("a")[0].get('href',None)
                            path = url.split("/catalog.html")[0] + "/" + link
                            size = i.select("td")[1].select("tt")[0].get_text().strip()
                            update = i.select("td")[2].select("tt")[0].get_text().strip()
                            print(link+path+size+update)
                            getUrl(path, name, dataType, size, update)
                            count = count + 1
                        elif "/" in name:
                            print("进入elif")
                            link = i.select("td")[0].select("a")[0].get('href',None)
                            path = url.split("/catalog.html")[0] + "/" + link
                            print("二次拼接path+++" + str(path))
                            getlist(path, dataType)

    except Exception as e:
        print("getlist(1) ----- " + str(e))


# 通过文件的Urldir确定catalog.html的位置，Urldir：如["alarm/", "SURF_CHN_MUL_HOR_N/", "SURF_CHN_MUL_HOR/", "SURF_CHN_MUL_DAY_N/", "SURF_CHN_MUL_DAY/", "LAPS/",  "GDFS/"]
# fileType：文件类型，如["灾害预警", "国家站小时", "自动站小时", "国家站天", "自动站天", "3KM实时网格", "5KM预报网格"]
def GetUrlAndDownloadsFileByType(Urldir, fileType):
    try:
        now = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        # 创建新目录
        # folder_path = "E:/project/dlqx/downloads/0903/{}".format(now)
        cp = configparser.ConfigParser()
        cp.read('config.ini',encoding="utf8")
        folder_path = ConfigParam.urlconfig['localdirpath']+now
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            # os.chdir("E:/project/dlqx/downloads/0903/{}".format(now))
            os.chdir(ConfigParam.urlconfig['localdirpath']+now)
        # 遍历接口
        # url = "http://220.243.128.38:8022/thredds/catalog/testAll/" + Urldir + "catalog.html"
        url = ConfigParam.urlconfig['jkurl'] + Urldir + "catalog.html"
        getlist(url, fileType)
    except Exception as e:
        print("GetUrlAndDownloadsFileByType ----- " + str(e))

# if __name__ == '__main__':
#     ll = ["alarm/", "SURF_CHN_MUL_HOR_N/", "SURF_CHN_MUL_HOR/", "SCW_CN/", "RADI_CHN_MUL_HOR/", "OCF/1H/",
#           "LAPS/GR2/","CG_QXZH/","HJ_QXZH/","DX_JBZH/"]
#     ntype = ["灾害预警", "国家站小时", "自动站小时", "SCW强对流预报", "辐射逐小时", "OCF逐小时预报", "3KM实时网格","常规气象灾害","环境气象灾害","电线积冰灾害"]
#     for i, m in zip(ll, ntype):
#         GetUrlAndDownloadsFileByType(i, m)
#     ybll = ["GDFS/SHYS_VIS/","GDFS/SFER_EDA10/","GDFS/QPF_R24/","GDFS/QPF_R12/","GDFS/QPF_R06/","GDFS/QPF_R03/",
#             "GDFS/QPF_PPH/","GDFS/OEFS_TMP/","GDFS/OEFS_TMIN/","GDFS/OEFS_TMAX/","GDFS/OEFS_RRH/","GDFS/OEFS_RHMX/","GDFS/OEFS_RHMI/","GDFS/OEFS_ECT/"]
#     for yb in ybll:
#         GetUrlAndDownloadsFileByType(yb,"5KM预报网格")

