import getDataUrlNew
import JxdataToMysql
import QxsjToJrsj
import datetime
import ConfigParam
import FileDataToMysql
import ncToMysql
from apscheduler.schedulers.blocking import BlockingScheduler
# coding=UTF-8
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
    

    print('tb_skzdzs24小时开始删除')
    JxdataToMysql.SkzdzsDelete()
    
    
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

#全局变量---用于监测灾害特征数据-常规气象灾害（CG_QXZH/)目录是否更新
CgqxzhHTMLTextNow = ""
#全局变量---用于监测灾害特征数据-环境气象灾害（HJ_QXZH/）是否更新
HjqxzhHTMLTextNow = ""
#全局变量---用于监测灾害特征数据-电线积冰灾害（DX_JBZH/）是否更新
DxjbzhHTMLTextNow = ""
#逐日任务----针对逐日更新的数据
def DayJob():
    print("DayTaskStartTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    global CgqxzhHTMLTextNow
    CgqxzhHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig['jkurl'] + "CG_QXZH/catalog.html")
    # 如果RADIHTMLText页面的内容---没有变，说明国家站小时数据没有更新
    # 如果RADIHTMLText页面的内容---有变化---将新的RADIHTMLText赋值给全局变量，并下载新数据到本机，解析到mysql
    if CgqxzhHTMLText != CgqxzhHTMLTextNow:
        CgqxzhHTMLTextNow = CgqxzhHTMLText
        # 爬取Url信息写入mysql，下载文件tar, nc，txt到本地文件夹中
        getDataUrlNew.GetUrlAndDownloadsFileByType("CG_QXZH/", "常规气象灾害")
        # # 调用java接口，将常规气象灾害txt数据存入Hbase中
        # requests.put("http://localhost:8080/InsertCgqxzhTxtToHbase")
        # tb_qxsj里面的数据处理之后存储进入tb_jrsj(应用服务器需要使用）
        # 找到tb_qxsj未同步到tb_jrsj接口数据
        qxsjsCgqxzh = QxsjToJrsj.GetQxsjByTypeNoTb("常规气象灾害")
        if qxsjsCgqxzh is not None and len(qxsjsCgqxzh) != 0:
            # tb_qxsj处理之后存储到tb_jrsj
            QxsjToJrsj.QxsjToJrsjCommon(qxsjsCgqxzh)
            # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsCgqxzh)
        # 常规气象灾害的txt解析存储到mysql表中（应用数据库中需要使用）
        # 获取未解析到mysql对应表中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
        jrsjCgqxzh = JxdataToMysql.GetJrsjByTypeNoJxStore("常规气象灾害")
        if jrsjCgqxzh is not None and len(jrsjCgqxzh) != 0:
            JxdataToMysql.JrsjCgqxzhJxIntoMysql(jrsjCgqxzh)
            JxdataToMysql.SetJrsjJxStatus1(jrsjCgqxzh)
        jrsjCgqxzhFile = FileDataToMysql.GetJrsjByTypeNoFileStore("常规气象灾害")
        if jrsjCgqxzhFile is not None and len(jrsjCgqxzhFile) != 0:
            FileDataToMysql.JrsjCgqxzhFilesIntoMysql(jrsjCgqxzhFile)
            FileDataToMysql.SetJrsjFileStoreStatus1(jrsjCgqxzhFile)


    global HjqxzhHTMLTextNow
    HjqxzhHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig['jkurl'] + "HJ_QXZH/catalog.html")
    # 如果RADIHTMLText页面的内容---没有变，说明国家站小时数据没有更新
    # 如果RADIHTMLText页面的内容---有变化---将新的RADIHTMLText赋值给全局变量，并下载新数据到本机，解析到mysql
    if HjqxzhHTMLText != HjqxzhHTMLTextNow:
        HjqxzhHTMLTextNow = HjqxzhHTMLText
        getDataUrlNew.GetUrlAndDownloadsFileByType("HJ_QXZH/", "环境气象灾害")
        # # 调用java接口，将环境气象灾害txt数据存入Hbase中
        # requests.put("http://localhost:8080/InsertHjqxzhTxtToHbase")
        qxsjsHjqxzh = QxsjToJrsj.GetQxsjByTypeNoTb("环境气象灾害")
        if qxsjsHjqxzh is not None and len(qxsjsHjqxzh) != 0:
            # tb_qxsj处理之后存储到tb_jrsj
            QxsjToJrsj.QxsjToJrsjCommon(qxsjsHjqxzh)
            # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsHjqxzh)
        jrsjHjqxzh = JxdataToMysql.GetJrsjByTypeNoJxStore("环境气象灾害")
        if jrsjHjqxzh is not None and len(jrsjHjqxzh) != 0:
            JxdataToMysql.JrsjHjqxzhJxIntoMysql(jrsjHjqxzh)
            JxdataToMysql.SetJrsjJxStatus1(jrsjHjqxzh)
        jrsjHjqxzhFile = FileDataToMysql.GetJrsjByTypeNoFileStore("环境气象灾害")
        if jrsjHjqxzhFile is not None and len(jrsjHjqxzhFile) != 0:
            FileDataToMysql.JrsjHjqxzhFilesIntoMysql(jrsjHjqxzhFile)
            FileDataToMysql.SetJrsjFileStoreStatus1(jrsjHjqxzhFile)

    global DxjbzhHTMLTextNow
    DxjbzhHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig['jkurl'] + "DX_JBZH/catalog.html")
    if DxjbzhHTMLText != DxjbzhHTMLTextNow:
        DxjbzhHTMLTextNow = DxjbzhHTMLText
        getDataUrlNew.GetUrlAndDownloadsFileByType("DX_JBZH/", "电线积冰灾害")
        # # 调用java接口，将电线积冰灾害txt数据存入Hbase中
        # requests.put("http://localhost:8080/InsertDxjbzhTxtToHbase")
        qxsjsDxjbzh = QxsjToJrsj.GetQxsjByTypeNoTb("电线积冰灾害")
        if qxsjsDxjbzh is not None and len(qxsjsDxjbzh) != 0:
            # tb_qxsj处理之后存储到tb_jrsj
            QxsjToJrsj.QxsjToJrsjCommon(qxsjsDxjbzh)
            # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsDxjbzh)
        jrsjDxjbzh = JxdataToMysql.GetJrsjByTypeNoJxStore("电线积冰灾害")
        if jrsjDxjbzh is not None and len(jrsjDxjbzh) != 0:
            JxdataToMysql.JrsjDxjbzhIntoMysql(jrsjDxjbzh)
            JxdataToMysql.SetJrsjJxStatus1(jrsjDxjbzh)
        jrsjDxjbzhFile = FileDataToMysql.GetJrsjByTypeNoFileStore("电线积冰灾害")
        if jrsjDxjbzhFile is not None and len(jrsjDxjbzhFile) != 0:
            FileDataToMysql.JrsjDxjbzhFilesIntoMysql(jrsjDxjbzhFile)
            FileDataToMysql.SetJrsjFileStoreStatus1(jrsjDxjbzhFile)

    print("DayTaskEndTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# 每日nc解析任务
# def NCJob():
#     print("NCTaskStartTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
#     ncToMysql.main()
#     print("NCTaskEndTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#全局变量---用于监测气象预报数据-OCF（OCF/1H/)目录是否更新
OCFHTMLTextNow = ""
#OCF 更新频次 6 8 12 20点
def OCFJob():

    print("OCFJobStartTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    global OCFHTMLTextNow
    OCFHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig['jkurl'] + "OCF/1H/catalog.html")
    if OCFHTMLText != OCFHTMLTextNow:
        OCFHTMLTextNow = OCFHTMLText
        getDataUrlNew.GetUrlAndDownloadsFileByType("OCF/1H/", "OCF逐小时预报")
        qxsjsOCF = QxsjToJrsj.GetQxsjByTypeNoTb("OCF逐小时预报")
        if qxsjsOCF is not None and len(qxsjsOCF) != 0:
            # tb_qxsj处理之后存储到tb_jrsj
            QxsjToJrsj.QxsjToJrsjOCF(qxsjsOCF)
            # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsOCF)
        # OCF逐小时预报json解析存储到mysql表中
        # 获取未解析到mysql对应表中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
        jrsjOCF = JxdataToMysql.GetJrsjByTypeNoJxStore("OCF逐小时预报")
        if jrsjOCF is not None and len(jrsjOCF) != 0:
            JxdataToMysql.JrsjOCFJxIntoMysql(jrsjOCF)
            JxdataToMysql.SetJrsjJxStatus1(jrsjOCF)
        jrsjOCFFile = FileDataToMysql.GetJrsjByTypeNoFileStore("OCF逐小时预报")
        if jrsjOCFFile is not None and len(jrsjOCFFile) != 0:
            FileDataToMysql.JrsjOcfHourFilesIntoMysql(jrsjOCFFile)
            FileDataToMysql.SetJrsjFileStoreStatus1(jrsjOCFFile)
    print("OCFJobEndTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))




#预报数据（四种,其中两种是8点和20点更新，还有一种逐小时更新，另一种5分钟更新一次）定时任务-每天8点和20点更新--nc数据未解析
def Predicttion0820Job():
    print("PredicttionNCJobStartTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    # # 重新爬取Url信息写入mysql，下载5KM预报网格nc文件到本地文件夹中
    ybll = ["GDFS/SHYS_VIS/", "GDFS/SFER_EDA10/", "GDFS/QPF_R24/", "GDFS/QPF_R12/", "GDFS/QPF_R06/", "GDFS/QPF_R03/",
            "GDFS/QPF_PPH/", "GDFS/OEFS_TMP/", "GDFS/OEFS_TMIN/", "GDFS/OEFS_TMAX/", "GDFS/OEFS_RRH/",
            "GDFS/OEFS_RHMX/", "GDFS/OEFS_RHMI/", "GDFS/OEFS_ECT/"]
    for yb in ybll:
        getDataUrlNew.GetUrlAndDownloadsFileByType(yb, "5KM预报网格")

    # # 调用java接口，将预报NC数据存入HDFS中
    # requests.put("http://localhost:8080/InsertPredictionNcToHdfs")
    # # 调用java接口，存储OCF逐小时预报
    # requests.put("http://localhost:8080/InsertOCFJsonToHbase")

    # tb_qxsj里面的数据处理之后存储进入tb_jrsj(应用服务器需要使用）
    # 找到tb_qxsj未同步到tb_jrsj接口数据
    qxsjsYb = QxsjToJrsj.GetQxsjByTypeNoTb("5KM预报网格")

    if qxsjsYb is not None and len(qxsjsYb) != 0:
        # tb_qxsj处理之后存储到tb_jrsj
        QxsjToJrsj.QxsjToJrsj5KMwg(qxsjsYb)
        # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
        QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsYb)

    print("PredicttionNCJobEndTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#全局变量---用于监测灾害数据是否更新
AlarmHTMLTextNow = ""
#灾害预警数据需要实时更新
def AlarmJob():
    print("-------------AlarmJobStartTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    global AlarmHTMLTextNow
    AlarmHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig["jkurl"]+ "alarm/catalog.html")
    #如果AlarmHTMLText页面的内容---没有变，说明灾害预警数据没有更新
    #如果AlarmHTMLText页面的内容---有变化---将新的AlarmHTMLText赋值给全局变量，并将灾害预警数据写入Hbase
    if AlarmHTMLText != AlarmHTMLTextNow:
        AlarmHTMLTextNow = AlarmHTMLText

        # #下载文件数据，接口写入tb_qxsj
        getDataUrlNew.GetUrlAndDownloadsFileByType("alarm/", "灾害预警")
        print(getDataUrlNew)
        # # 调用java接口，将灾害数据存入Hbase中
        # requests.put("http://localhost:8080/InsertAlarmXmlToHbase");

        # tb_qxsj里面的数据处理之后存储进入tb_jrsj(应用服务器需要使用）
        # 找到tb_qxsj未同步到tb_jrsj接口数据
        qxsjsZhyj = QxsjToJrsj.GetQxsjByTypeNoTb("灾害预警")
        if qxsjsZhyj is not None and len(qxsjsZhyj) != 0:
            # tb_qxsj处理之后存储到tb_jrsj
            QxsjToJrsj.QxsjToJrsjAlarm(qxsjsZhyj)
            # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsZhyj)

        # 将灾害预警的xml解析存储到tb_alarm表中（应用数据库中需要使用）
        # 获取未解析到tb_alarm中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
        jrsjAlarm = JxdataToMysql.GetJrsjByTypeNoJxStore("灾害预警")
        print("-----------修改标志位----------",jrsjAlarm)
        if jrsjAlarm is not None and len(jrsjAlarm) != 0:
            # 解析数据存入mysql
            print('-----------修改标志位2222----------',jrsjAlarm)
            JxdataToMysql.JrsjAlarmJxIntoMysql(jrsjAlarm)
            # 解析处理之后的接入数据接口tb_jrsj,将标志位更新为1,
            JxdataToMysql.SetJrsjJxStatus1(jrsjAlarm)

        # 文件内容不解析直接存储到mysql中，存储非结构化的文本内容
        jrsjAlarmFile = FileDataToMysql.GetJrsjByTypeNoFileStore("灾害预警")
        if jrsjAlarmFile is not None and len(jrsjAlarmFile) != 0:
            FileDataToMysql.JrsjAlarmFilesIntoMysql(jrsjAlarmFile)
            FileDataToMysql.SetJrsjFileStoreStatus1(jrsjAlarmFile)
    

    # print('开始删除alarm24小时前的数据')
    # JxdataToMysql.AlarmDelete()
    

    print("AlarmJobEndTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


#全局变量---用于监测灾害特征数据-常规气象灾害（CG_QXZH/)目录是否更新
SCWHTMLTextNow = ""
#SCW定时任务-每5分钟更新一次
def SCWJob():
    print("SCWJobStartTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    global SCWHTMLTextNow
    SCWHTMLText = getDataUrlNew.getHTMLText(ConfigParam.urlconfig['jkurl'] + "SCW_CN/catalog.html")
    if SCWHTMLText != SCWHTMLTextNow:
        SCWHTMLTextNow = SCWHTMLText
        # # 重新爬取Url信息写入mysql，下载5KM预报网格SCW文件到本地文件夹中
        getDataUrlNew.GetUrlAndDownloadsFileByType("SCW_CN/", "SCW强对流预报")
        print("getDataUrlNew++" + str(getDataUrlNew))
        # # 调用java接口，存储SCW强对流预报
        # requests.put("http://localhost:8080/InsertSCWJsonToHbase")

        # tb_qxsj里面的数据处理之后存储进入tb_jrsj(应用服务器需要使用）
        # 找到tb_qxsj未同步到tb_jrsj接口数据
        qxsjsSCW = QxsjToJrsj.GetQxsjByTypeNoTb("SCW强对流预报")
        print("qxsjsSCW" + str(qxsjsSCW))
        if qxsjsSCW is not None and len(qxsjsSCW) != 0:
            QxsjToJrsj.QxsjToJrsjCommon(qxsjsSCW)
            QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsSCW)

        # # SCWjson解析存储到mysql表中---解析未完成
        # # 获取未解析到mysql对应表中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
        # jrsjSCW = JxdataToMysql.GetJrsjByTypeNoJxStore("SCW强对流预报")
        # if len(jrsjSCW) != 0:
        #     JxdataToMysql.JrsjSCWJxIntoMysql(jrsjSCW)
        #     JxdataToMysql.SetJrsjJxStatus1(jrsjSCW)
        # 文件内容不解析直接存储到mysql中，存储非结构化的文本内容
        jrsjSCWFile = FileDataToMysql.GetJrsjByTypeNoFileStore("SCW强对流预报")
        print("jrsjSCWFile" + str(jrsjSCWFile))
        if jrsjSCWFile is not None and len(jrsjSCWFile) != 0:
            FileDataToMysql.JrsjSCWHourFilesIntoMysql(jrsjSCWFile)
            FileDataToMysql.SetJrsjFileStoreStatus1(jrsjSCWFile)
            # scw解析之后存储到tb_scw
            JxdataToMysql.JrsjSCWJxIntoMysql(jrsjSCWFile)
            JxdataToMysql.SetJrsjJxStatus1(jrsjSCWFile)


    print("SCWJobEndTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
# 每日nc解析任务
def NCJob():
    print("NCTaskStartTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print('GDFS方法开始执行********************************************************************************************************************************************')
    ncToMysql.main()
    print("NCTaskEndTime:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    # # cron定时器：每小时的32,33,34,35 40,41,42分钟 执行一次
    # scheduler.add_job(HourJob, 'cron', id="HourJob", minute='30')
    #
    # scheduler.add_job(DayJob, 'cron', id="DayJob", hour='0',minute='0,5,10')
    #
    # scheduler.add_job(OCFJob, 'interval', id="OCFJob", start_date='2020-09-11 00:00:00', minutes=5);
    # scheduler.add_job(Predicttion0820Job, 'cron', id="Predicttion0820Job", minute='8，10')
    #
    # scheduler.add_job(SCWJob, 'interval', id="SCWJob", start_date='2020-09-11 00:00:00', minute=5);
    #
    # scheduler.add_job(AlarmJob, 'interval', id="AlarmJob", start_date='2020-09-11 00:00:00', minute=2);

    # 正式用cron定时器：
    # 相当于 每小时任务：每小时的35分执行,）



    scheduler.add_job(HourJob, 'cron', id="HourJob", minute='40')
    # 每天的0点执行：
    scheduler.add_job(DayJob, 'cron', id="DayJob", hour='0')
    #scheduler.add_job(DayJob, 'cron', id="DayJob", minute='5')
    # 每天的6 8 12 20点执行：
    scheduler.add_job(OCFJob, 'cron', id="OCFJob", hour='6,8,12,20')
    #scheduler.add_job(OCFJob, 'cron', id="OCFJob", minute='10')
    # 每天的8点，20点执行：
    scheduler.add_job(Predicttion0820Job, 'cron', id="Predicttion0820Job", hour='8, 20')
    #scheduler.add_job(Predicttion0820Job, 'cron', id="Predicttion0820Job", minute='15')
    # 每5分钟执行一次：
    scheduler.add_job(SCWJob, 'interval', id="SCWJob", start_date='2020-09-11 00:00:00', minutes=5)
    # 每10分钟执行一次(扫一次他们的表时间有些长，换成十分钟执行一次)：
    scheduler.add_job(AlarmJob, 'interval', id="AlarmJob", start_date='2020-09-11 00:00:00',minutes=10)
    # 每天的3点执行：
    scheduler.add_job(NCJob, 'cron', id="NCJob", hour='3')

    scheduler.start()

    # Predicttion0820Job()
    # HourJob()
    # OCFJob()
    # DayJob()
    # SCWJob()
    # AlarmJob()




