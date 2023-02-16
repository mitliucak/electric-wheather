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

def DaquLogJrsjIntoMysql(type, failinfo, status, url, name, dataType, size, update):
    jkinfo = {"url": url,
              "name": name,
              "datatype": dataType,
              "size": size,
              "update": update}
    if status == 1:
        desc = "接入数据" + name + "下载成功, 接口信息:" + str(jkinfo)
    else:
        desc = "接入数据" + name + "下载失败, 接口信息:" + str(jkinfo)

    logtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insertlogsql = "INSERT INTO tb_daqulog(daqulog_time,daqulog_type,daqulog_failinfo,daqulog_desc,daqulog_status) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")" % (
    logtime, type, failinfo, desc, status)
    try:
        conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
        cursor3 = conn3.cursor()
        cursor3.execute(insertlogsql)
        # conn3.commit()
    except Exception as e:
        print("DaquLogJrsjIntoMysql ----- ")
    finally:
        cursor3.close()
        conn3.close()


def getHTMLText(url):
    print(url)
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = 'utf-8'
        r.raise_for_status()
        return r.text
    except Exception as e:
        print("getHTMLText ----- ")
        print(e)


def unzip():
    sql = 'SELECT id, qxsj_path FROM tb_qxsj'
    try:
        conn = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor = conn.cursor()
        cursor.execute(sql)
        # conn.commit()
        qxsj = cursor.fetchall()
        for one in qxsj:
            filepath = one[1].replace("\\", "/")
            dest_dir = filepath.rsplit("/", 1)[0]
            if ".tar" in filepath:
                tar = tarfile.open(filepath, 'r')
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
        print("unzip ----- ")
    finally:
        cursor.close()
        conn.close()


# 根据文件名判断该接口是否已经写入mysql
def FileExist(filename, update):
    try:
        conn = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
        cursor = conn.cursor()
        sql = "SELECT * FROM tb_qxsj WHERE  qxsj_name = '%s' AND qxsj_update = '%s'" % (filename, update)
        cursor.execute(sql)
        # conn.commit()
        qxsj = cursor.fetchall()
    except Exception as e:
        print("FileExist(2) ----- ")
        print(e)
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


# 检查接口状态，
# def checkJkStatus(path, name, dataType, size, update):
#     if


def saveLink(path, name, dataType, size, update):
    # print(path, name, dataType, size, update)
    # 当前目录
    current_path = os.getcwd()
    # print(current_path)
    # 下载文件
    Download_addres = path
    file = requests.get(Download_addres)
    with open(name, "wb") as code:
        code.write(file.content)
        file_path1 = current_path + "/" + name
        # file_path = file_path1.strip().replace('\\','\\\\')
        # Linux 下文件目录 '/' ,windows默认 '\', windows'/' 也可以识别无空格即可
        file_path = file_path1.strip().replace('\\', '/')
        # 文件下载结束后，若文件名中有.tar 或者.zip 进行解压
        # #=============================连接数据库==================================
        # 结构化数据写入第1个mysql
        # print(dataType)
        sql = "INSERT INTO tb_qxsj(qxsj_url,qxsj_name,qxsj_type,qxsj_size,qxsj_update,qxsj_path) \
           VALUES ('%s','%s','%s','%s','%s','%s')" % (path,name,dataType,size,update,file_path)
        try:
            conn1 = jaydebeapi.connect(driver, jdbc_url1, uandp, jar_file)
            cursor1 = conn1.cursor()
            cursor1.execute(sql)
            print('tb_qxsj存入成功1111111111',name)
            # conn1.commit()
        except Exception as e:
            print("saveLink(1) ----- e")
        finally:
            cursor1.close()
            conn1.close()
        # 结构化数据写入第2个mysql
        try:
            conn2 = jaydebeapi.connect(driver, jdbc_url2, uandp, jar_file)
            cursor2 = conn2.cursor()
            cursor2.execute(sql)
            print('tb_qxsj存入成功22222222222',name)
            # conn2.commit()
        except Exception as e:
            print("saveLink(2) ----- e")
        finally:
            cursor2.close()
            conn2.close()
        # 结构化数据写入第3个mysql
        try:
            conn3 = jaydebeapi.connect(driver, jdbc_url3, uandp, jar_file)
            cursor3 = conn3.cursor()
            cursor3.execute(sql)
            print('tb_qxsj存入成功3333333',name)
            # conn3.commit()
        except Exception as e:
            print("saveLink(3) ----- e")
        finally:
            cursor3.close()
            conn3.close()


def getUrl(url, name, dataType, size, update):
    try:
        html = getHTMLText(url)
        soup = BeautifulSoup(html, "lxml")
        data = soup.select("ol li")
        for i in data:
            if i.select("b")[0].get_text().strip() == "HTTPServer:":
                link = i.select("a")[0].get('href', None)
                path = url.split("/thredds")[0] + link
                # saveLink(path, name, dataType, size, update)
                # 若该接口以前没有写入mysql则写入，如果曾经写入过则不重新写入避免数据重复
                try:
                    if FileExist(name, update) == 0:
                        saveLink(path, name, dataType, size, update)
                except Exception as e:
                    print("getUrl(1) ----- ")
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
    # print(url)
    try:
        html = getHTMLText(url)
        # print(html)
        soup = BeautifulSoup(html, "lxml")
        # print(soup)
        data = soup.select("tr")
        # print(data)
        for i in data:
            try:
                if len(i.select("td")) > 0:
                    name = i.select("td")[0].select("tt")[0].get_text().strip()
                    if ".tar.gz" in name or ".zip" in name or ".TXT" in name or ".nc" in name:
                        link = i.select("td")[0].select("a")[0].get('href', None)
                        path = url.split("/catalog.html")[0] + "/" + link
                        size = i.select("td")[1].select("tt")[0].get_text().strip()
                        update = i.select("td")[2].select("tt")[0].get_text().strip()
                        getUrl(path, name, dataType, size, update)
                    elif "/" in name:
                        link = i.select("td")[0].select("a")[0].get('href', None)
                        path = url.split("/catalog.html")[0] + "/" + link
                        getlist(path, dataType)
            except Exception as e:
                print("getlist(2) ----- ")
                print(e.args)
    except Exception as e:
        print("getlist(1) ----- ")
        print(e)


# 通过文件的Urldir确定catalog.html的位置，Urldir：如["alarm/", "SURF_CHN_MUL_HOR_N/", "SURF_CHN_MUL_HOR/", "SURF_CHN_MUL_DAY_N/", "SURF_CHN_MUL_DAY/", "LAPS/",  "GDFS/"]
# fileType：文件类型，如["灾害预警", "国家站小时", "自动站小时", "国家站天", "自动站天", "3KM实时网格", "5KM预报网格"]
def GetUrlAndDownloadsFileByType(Urldir, fileType):
    if fileType == '常规气象灾害' or fileType == '环境气象灾害' or fileType == '电线积冰灾害' or fileType == '3KM实时网格' or fileType == 'OCF逐小时预报':
        try:
            now = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
            # 创建新目录
            # folder_path = "E:/project/dlqx/downloads/0903/{}".format(now)
            cp = configparser.ConfigParser()
            cp.read('config.ini', encoding="utf8")
            folder_path = ConfigParam.urlconfig['localdirpath'] + now
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                # os.chdir("E:/project/dlqx/downloads/0903/{}".format(now))
                os.chdir(ConfigParam.urlconfig['localdirpath'] + now)
            # 遍历接口
            # url = "http://220.243.128.38:8022/thredds/catalog/testAll/" + Urldir + "catalog.html"
            url = ConfigParam.urlconfig['jkurl'] + Urldir + "catalog.html"
            getlist(url, fileType)
        except Exception as e:
            print("GetUrlAndDownloadsFileByType ----- ")
    else:
        try:
            print('1111111')
            url = ConfigParam.urlconfig['jkurl'] + Urldir + "catalog.html"
            print('222222222222222')
            html = getHTMLText(url)
            print(html)
            print('33333333333333333333333')
            soup = BeautifulSoup(html, "lxml")
            # print(soup)
            data = soup.select("tr")
            for i in data:
                try:
                    timeStr = ''
                    dataName = ''
                    if len(i.select("td")) > 0:
                        ttName = i.select("td")[0].select("tt")[0].get_text().strip()
                        if '/' in ttName:
                            timeStr = ttName.split('/')[0]
                        else:
                            dataName = ttName
                        # 创建新目录
                        cp = configparser.ConfigParser()
                        cp.read('config.ini', encoding="utf8")
                        folder_path = ConfigParam.urlconfig['localdirpath'] + dataName
                        if not os.path.exists(folder_path):
                            os.makedirs(folder_path)
                            # print(folder_path)
                        os.chdir(folder_path)
                        # 遍历接口
                        url1 = ConfigParam.urlconfig['jkurl'] + Urldir + timeStr + "/catalog.html"
                        # print(url1)
                        getlist(url1, fileType)
                except Exception as e:
                    print("GetUrlAndDownloadsFileByType(2.2) ----- ")
                    print(e)
        except Exception as e:
            print("GetUrlAndDownloadsFileByType(2.1) ----- ")
            print(e)


if __name__ == '__main__':
    ll = ["alarm/", "SURF_CHN_MUL_HOR_N/", "SURF_CHN_MUL_HOR/", "SCW_CN/", "RADI_CHN_MUL_HOR/", "OCF/1H/",
          "LAPS/GR2/", "CG_QXZH/", "HJ_QXZH/", "DX_JBZH/"]
    ntype = ["灾害预警", "国家站小时", "自动站小时", "SCW强对流预报", "辐射逐小时", "OCF逐小时预报", "3KM实时网格", "常规气象灾害", "环境气象灾害", "电线积冰灾害"]
    for i, m in zip(ll, ntype):
        GetUrlAndDownloadsFileByType(i, m)
    # ybll = ["GDFS/SHYS_VIS/","GDFS/SFER_EDA10/","GDFS/QPF_R24/","GDFS/QPF_R12/","GDFS/QPF_R06/","GDFS/QPF_R03/",
    #         "GDFS/QPF_PPH/","GDFS/OEFS_TMP/","GDFS/OEFS_TMIN/","GDFS/OEFS_TMAX/","GDFS/OEFS_RRH/","GDFS/OEFS_RHMX/","GDFS/OEFS_RHMI/","GDFS/OEFS_ECT/"]
    # for yb in ybll:
    #     GetUrlAndDownloadsFileByType(yb,"5KM预报网格")

