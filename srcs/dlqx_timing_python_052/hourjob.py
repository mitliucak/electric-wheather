import getDataUrlNew
import JxdataToMysql
import QxsjToJrsj
import requests
import datetime
import ConfigParam
import FileDataToMysql
import jpype
from apscheduler.schedulers.blocking import BlockingScheduler

#全局变量---用于监测实时数据-国家站小时（SURF_CHN_MUL_HOR_N)目录是否更新
GJHourHTMLTextNow = ""
#全局变量---用于监测实时数据-自动站小时数据（SURF_CHN_MUL_HOR）是否更新
ZDHourHTMLTextNow = ""
#全局变量---用于监测实时数据-3KM实时网格（LAPS/GR2/）是否更新
LAPSHTMLTextNow = ""
#全局变量---用于监测预报数据-辐射逐小时（RADI_CHN_MUL_HOR/）是否更新
RADIHTMLTextNow = ""
#逐小时任务----针对逐小时更新的数据
def HourJob():
    print("HourTaskStartTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    global GJHourHTMLTextNow
    GJHourHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig['jkurl'] + "SURF_CHN_MUL_HOR_N/catalog.html")
    # 如果GJHourHTMLText页面的内容---没有变，说明国家站小时数据没有更新
    # 如果GJHourHTMLText页面的内容---有变化---将新的GJHourHTMLText赋值给全局变量，并下载新数据到本机，解析到mysql
    if GJHourHTMLText != GJHourHTMLTextNow:
        GJHourHTMLTextNow = GJHourHTMLText
        # 爬取Url信息写入mysql，下载文件tar, nc，txt到本地文件夹中
        getDataUrlNew.GetUrlAndDownloadsFileByType("SURF_CHN_MUL_HOR_N/", "国家站小时")
        # # 调用java接口，将逐小时站点txt数据（国家站小时，自动站小时）存入Hbase中
        # requests.put("http://localhost:8080/InsertSssjZdHourTxtToHbase");
        # tb_qxsj里面的数据处理之后存储进入tb_jrsj(应用服务器需要使用）
        # 找到tb_qxsj未同步到tb_jrsj接口数据
        qxsjsGjxs = QxsjToJrsj.GetQxsjByTypeNoTb("国家站小时")
        if qxsjsGjxs is not None and len(qxsjsGjxs) != 0:
            # tb_qxsj处理之后存储到tb_jrsj
            QxsjToJrsj.QxsjToJrsjZd(qxsjsGjxs)
            # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsGjxs)
        # 将国家站小时，自动站小时, 辐射逐小时的txt解析存储到tb_skzdzs_jx表中（应用数据库中需要使用）
        # 获取未解析到tb_skzdzs_jx中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
        jrsjGjxs = JxdataToMysql.GetJrsjByTypeNoJxStore("国家站小时")
        if jrsjGjxs is not None and len(jrsjGjxs) != 0:
            JxdataToMysql.JrsjSkzszsJxIntoMysql(jrsjGjxs)
            JxdataToMysql.SetJrsjJxStatus1(jrsjGjxs)
        # 文件内容不解析直接存储到mysql中，存储非结构化的文本内容
        # 获取未存储到tb_gjzhourfile中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
        jrsjGjzHourFile = FileDataToMysql.GetJrsjByTypeNoFileStore("国家站小时")
        # 将对应的文件数据存储到对应的非结构化表中，并将标志位jrsj_filestorestatus置为1
        if jrsjGjzHourFile is not None and len(jrsjGjzHourFile) != 0:
            FileDataToMysql.JrsjGjzHourFilesIntoMysql(jrsjGjzHourFile)
            FileDataToMysql.SetJrsjFileStoreStatus1(jrsjGjzHourFile)

    global ZDHourHTMLTextNow
    ZDHourHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig['jkurl'] + "SURF_CHN_MUL_HOR/catalog.html")
    # 如果ZDHourHTMLText页面的内容---没有变，说明国家站小时数据没有更新
    # 如果ZDHourHTMLText页面的内容---有变化---将新的ZDHourHTMLText赋值给全局变量，并下载新数据到本机，解析到mysql
    if ZDHourHTMLText != ZDHourHTMLTextNow:
        ZDHourHTMLTextNow = ZDHourHTMLText
        getDataUrlNew.GetUrlAndDownloadsFileByType("SURF_CHN_MUL_HOR/", "自动站小时")
        # # 调用java接口，将逐小时站点txt数据（国家站小时，自动站小时）存入Hbase中
        # requests.put("http://localhost:8080/InsertSssjZdHourTxtToHbase");
        qxsjsZdxs = QxsjToJrsj.GetQxsjByTypeNoTb("自动站小时")
        if qxsjsZdxs is not None and len(qxsjsZdxs) != 0:
            QxsjToJrsj.QxsjToJrsjZd(qxsjsZdxs)
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsZdxs)
        jrsjZdxs = JxdataToMysql.GetJrsjByTypeNoJxStore("自动站小时")
        if jrsjZdxs is not None and len(jrsjZdxs) != 0:
            JxdataToMysql.JrsjSkzszsJxIntoMysql(jrsjZdxs)
            JxdataToMysql.SetJrsjJxStatus1(jrsjZdxs)
        jrsjZdzHourFile = FileDataToMysql.GetJrsjByTypeNoFileStore("自动站小时")
        if jrsjZdzHourFile is not None and len(jrsjZdzHourFile) != 0:
            FileDataToMysql.JrsjZdzHourFilesIntoMysql(jrsjZdzHourFile)
            FileDataToMysql.SetJrsjFileStoreStatus1(jrsjZdzHourFile)

    global LAPSHTMLTextNow
    LAPSHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig['jkurl'] + "LAPS/GR2/catalog.html")
    # 如果LAPSHTMLText页面的内容---没有变，说明国家站小时数据没有更新
    # 如果LAPSHTMLText页面的内容---有变化---将新的LAPSHTMLText赋值给全局变量，并下载新数据到本机，解析到mysql
    if LAPSHTMLText != LAPSHTMLTextNow:
        LAPSHTMLTextNow = LAPSHTMLText
        getDataUrlNew.GetUrlAndDownloadsFileByType("LAPS/GR2/", "3KM实时网格")
        # # 调用java接口，将实况NC数据存入HDFS中
        # requests.put("http://localhost:8080/InsertActualNcToHdfs");
        qxsjsWg3km = QxsjToJrsj.GetQxsjByTypeNoTb("3KM实时网格")
        if qxsjsWg3km is not None and len(qxsjsWg3km) != 0:
            QxsjToJrsj.QxsjToJrsj3KMwg(qxsjsWg3km)
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsWg3km)

    global RADIHTMLTextNow
    RADIHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig['jkurl'] + "RADI_CHN_MUL_HOR/catalog.html")
    # 如果RADIHTMLText页面的内容---没有变，说明国家站小时数据没有更新
    # 如果RADIHTMLText页面的内容---有变化---将新的RADIHTMLText赋值给全局变量，并下载新数据到本机，解析到mysql
    if RADIHTMLText != RADIHTMLTextNow:
        RADIHTMLTextNow = RADIHTMLText
        getDataUrlNew.GetUrlAndDownloadsFileByType("RADI_CHN_MUL_HOR/", "辐射逐小时")
        # # 调用java接口，存储辐射逐小时
        # requests.put("http://localhost:8080/InsertRadiTxtToHbase")
        qxsjsRadi = QxsjToJrsj.GetQxsjByTypeNoTb("辐射逐小时")
        if qxsjsRadi is not None and len(qxsjsRadi) != 0:
            QxsjToJrsj.QxsjToJrsjCommon(qxsjsRadi)
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsRadi)
            # 获取未解析到tb_radi中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
        jrsjRadi = JxdataToMysql.GetJrsjByTypeNoJxStore("辐射逐小时")
        # 此处放辐射追小时txt解析写入tb_radi
        if jrsjRadi is not None and len(jrsjRadi) != 0:
            JxdataToMysql.JrsjRadiJxIntoMysql(jrsjRadi)
            JxdataToMysql.SetJrsjJxStatus1(jrsjRadi)
        jrsjRadiFile = FileDataToMysql.GetJrsjByTypeNoFileStore("辐射逐小时")
        if jrsjRadiFile is not None and len(jrsjRadiFile) != 0:
            FileDataToMysql.JrsjRadiHourFilesIntoMysql(jrsjRadiFile)
            FileDataToMysql.SetJrsjFileStoreStatus1(jrsjRadiFile)


    print("HourTaskEndTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    HourJob()