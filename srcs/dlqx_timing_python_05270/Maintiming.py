import getDataUrlNew
import JxdataToMysql
import QxsjToJrsj
import datetime
import ConfigParam
import FileDataToMysql
import ncToMysql
from apscheduler.schedulers.blocking import BlockingScheduler
# coding=UTF-8
def QxhjSssj():
    print('***************************************************')
    # 爬取Url信息写入mysql，下载文件tar, nc，txt到本地文件夹中
    # getDataUrlNew.GetUrlAndDownloadsFileByType("SURF_CHN_MUL_HOR_N/", "国家站小时")

    # getDataUrlNew.GetUrlAndDownloadsFileByType("SURF_CHN_MUL_HOR/", "自动站小时")
    #
    # getDataUrlNew.GetUrlAndDownloadsFileByType("LAPS/GR2/", "3KM实时网格")

    print('---------------------11111111111111----------------------------------')
    # tb_qxsj里面的数据处理之后存储进入tb_jrsj(应用服务器需要使用）
    # 找到tb_qxsj未同步到tb_jrsj接口数据
    # qxsjsGjxs = QxsjToJrsj.GetQxsjByTypeNoTb("国家站小时")
    print('3333333333333333333333333333333333333333333333333333333333333333333333')
    # print(qxsjsGjxs)
    # qxsjsZdxs = QxsjToJrsj.GetQxsjByTypeNoTb("自动站小时")
    print('4444444444444444444444444444444444444444444444444444444444444444444444444444444')
    # print(qxsjsZdxs)
    qxsjsWg3km = QxsjToJrsj.GetQxsjByTypeNoTb("3KM实时网格")
    print('5555555555555555555555555555555555555555555555555555555555555555555555555555555555555')
    print(qxsjsWg3km)
    print('-----------------------------------------------------------------------------------------------')

    # if qxsjsGjxs is not None and len(qxsjsGjxs) != 0:
        # tb_qxsj处理之后存储到tb_jrsj
        # QxsjToJrsj.QxsjToJrsjZd(qxsjsGjxs)
        # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
        # QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsGjxs)
    # if qxsjsZdxs is not None and len(qxsjsZdxs) != 0:
    #     QxsjToJrsj.QxsjToJrsjZd(qxsjsZdxs)
    #     QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsZdxs)
    if qxsjsWg3km is not None and len(qxsjsWg3km) != 0:
        QxsjToJrsj.QxsjToJrsj3KMwg(qxsjsWg3km)
    #     QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsWg3km)
    print('-----------------------------------------------------------------------------------------')

    # 网格数据暂定
    # 文件内容不解析直接存储到mysql中，存储非结构化的文本内容
    # 获取未存储到tb_gjzhourfile中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
    # jrsjGjzHour = FileDataToMysql.GetJrsjByTypeNoFileStore("国家站小时")
    # print('99999999999999999999999999999999999999999999999999999999999999999999999999999999999999')
    # print(jrsjGjzHour)
    # # 获取未存储到tb_zdzhourfile中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
    # jrsjZdzHour = FileDataToMysql.GetJrsjByTypeNoFileStore("自动站小时")
    # print('1010101010101010101010101010101010101010101010101010101010101011010100001010101010110101010101010')
    # # print(jrsjZdzHour)
    # # 将对应的文件数据存储到对应的非结构化表中，并将标志位jrsj_filestorestatus置为1
    # print('----------------------------------------------------------------------------------------------------')
    # if jrsjGjzHour is not None and len(jrsjGjzHour) != 0:
    #     FileDataToMysql.JrsjGjzHourFilesIntoMysql(jrsjGjzHour)
    #     FileDataToMysql.SetJrsjFileStoreStatus1(jrsjGjzHour)
    # if jrsjZdzHour is not None and len(jrsjZdzHour) != 0:
    #     FileDataToMysql.JrsjZdzHourFilesIntoMysql(jrsjZdzHour)
    #     FileDataToMysql.SetJrsjFileStoreStatus1(jrsjZdzHour)
    #
    # # 将国家站小时，自动站小时的txt解析存储到tb_skzdzs_jx表中（应用数据库中需要使用）
    # # 获取未解析到tb_skzdzs_jx中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
    # print('----------------------------------------------------------------------------------------------------')
    # jrsjGjxs = JxdataToMysql.GetJrsjByTypeNoJxStore("国家站小时")
    print('11*11*11*11*111*1*11*11*11*11*11*11*11*11*11*11*11*11*11*11*11*11*11*11*11*11*11*11*11')
    # print(jrsjGjxs)
    # jrsjZdxs = JxdataToMysql.GetJrsjByTypeNoJxStore("自动站小时")
    print('12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12*12')
    # print(jrsjZdxs)
    # print('--------------------------------------------------------------------------------------------------')
    # jrsjRadi = JxdataToMysql.GetJrsjByTypeNoJxStore("辐射逐小时")
    # if jrsjGjxs is not None and len(jrsjGjxs) != 0:
    #     JxdataToMysql.JrsjSkzszsJxIntoMysql(jrsjGjxs)
    #     JxdataToMysql.SetJrsjJxStatus1(jrsjGjxs)
    # if jrsjZdxs is not None and len(jrsjZdxs) != 0:
        # JxdataToMysql.JrsjSkzszsJxIntoMysql(jrsjZdxs)
        # JxdataToMysql.SetJrsjJxStatus1(jrsjZdxs)
    print('***************************************************************************************************************')

#气象灾害特征数据-获取-存储-同步解析数据(供应用服务器应用）
def Qxzhtzsj():
    # 重新爬取Url信息写入mysql，下载文件tar, nc，txt到本地文件夹中
    # getDataUrlNew.GetUrlAndDownloadsFileByType("CG_QXZH/", "常规气象灾害")
    # getDataUrlNew.GetUrlAndDownloadsFileByType("HJ_QXZH/", "环境气象灾害")
    # getDataUrlNew.GetUrlAndDownloadsFileByType("DX_JBZH/", "电线积冰灾害")

    # tb_qxsj里面的数据处理之后存储进入tb_jrsj(应用服务器需要使用）
    # 找到tb_qxsj未同步到tb_jrsj接口数据
    # qxsjsCgqxzh = QxsjToJrsj.GetQxsjByTypeNoTb("常规气象灾害")
    # print(qxsjsCgqxzh)
    # qxsjsHjqxzh = QxsjToJrsj.GetQxsjByTypeNoTb("环境气象灾害")
    # print(qxsjsHjqxzh)
    qxsjsDxjbzh = QxsjToJrsj.GetQxsjByTypeNoTb("电线积冰灾害")
    print(qxsjsDxjbzh)
    # if qxsjsCgqxzh is not None and len(qxsjsCgqxzh) != 0:
        # tb_qxsj处理之后存储到tb_jrsj
        # QxsjToJrsj.QxsjToJrsjCommon(qxsjsCgqxzh)
        # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
        # QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsCgqxzh)
    # if qxsjsHjqxzh is not None and len(qxsjsHjqxzh) != 0:
        # dtb_qxsj处理之后存储到tb_jrsj
        # QxsjToJrsj.QxsjToJrsjCommon(qxsjsHjqxzh)
        # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
        # QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsHjqxzh)
    if qxsjsDxjbzh is not None and len(qxsjsDxjbzh) != 0:
        # tb_qxsj处理之后存储到tb_jrsj
        QxsjToJrsj.QxsjToJrsjCommon(qxsjsDxjbzh)
        # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
        # QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsDxjbzh)

    # 文件内容不解析直接存储到mysql中，存储非结构化的文本内容
    # jrsjCgqxzhFile = FileDataToMysql.GetJrsjByTypeNoFileStore("常规气象灾害")
    # jrsjHjqxzhFile = FileDataToMysql.GetJrsjByTypeNoFileStore("环境气象灾害")
    jrsjDxjbzhFile = FileDataToMysql.GetJrsjByTypeNoFileStore("电线积冰灾害")
    # if jrsjCgqxzhFile is not None and len(jrsjCgqxzhFile) != 0:
        # FileDataToMysql.JrsjCgqxzhFilesIntoMysql(jrsjCgqxzhFile)
        # FileDataToMysql.SetJrsjFileStoreStatus1(jrsjCgqxzhFile)
    # if jrsjHjqxzhFile is not None and len(jrsjHjqxzhFile) != 0:
        # FileDataToMysql.JrsjHjqxzhFilesIntoMysql(jrsjHjqxzhFile)
        # FileDataToMysql.SetJrsjFileStoreStatus1(jrsjHjqxzhFile)
    if jrsjDxjbzhFile is not None and len(jrsjDxjbzhFile) != 0:
        FileDataToMysql.JrsjDxjbzhFilesIntoMysql(jrsjDxjbzhFile)
        # FileDataToMysql.SetJrsjFileStoreStatus1(jrsjDxjbzhFile)

    # 常规气象灾害的txt解析存储到mysql表中（应用数据库中需要使用）
    # 获取未解析到mysql对应表中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
    # jrsjCgqxzh = JxdataToMysql.GetJrsjByTypeNoJxStore("常规气象灾害")
    # jrsjHjqxzh = JxdataToMysql.GetJrsjByTypeNoJxStore("环境气象灾害")
    # jrsjDxjbzh = JxdataToMysql.GetJrsjByTypeNoJxStore("电线积冰灾害")
    # if jrsjCgqxzh is not None and len(jrsjCgqxzh) != 0:
        # JxdataToMysql.JrsjCgqxzhJxIntoMysql(jrsjCgqxzh)
        # JxdataToMysql.SetJrsjJxStatus1(jrsjCgqxzh)
    # if jrsjHjqxzh is not None and len(jrsjHjqxzh) != 0:
        # JxdataToMysql.JrsjHjqxzhJxIntoMysql(jrsjHjqxzh)
        # JxdataToMysql.SetJrsjJxStatus1(jrsjHjqxzh)
    jrsjDxjbzh = JxdataToMysql.GetJrsjByTypeNoJxStore("电线积冰灾害")
    if jrsjDxjbzh is not None and len(jrsjDxjbzh) != 0:
        JxdataToMysql.JrsjDxjbzhIntoMysql(jrsjDxjbzh)
        # JxdataToMysql.SetJrsjJxStatus1(jrsjDxjbzh)

#气象预报信息数据-获取-存储-同步解析数据(供应用服务器应用）
def Qxybxxsj():
    #重新爬取Url信息写入mysql，下载5KM预报网格nc文件到本地文件夹中
    # ybll = ["GDFS/SHYS_VIS/", "GDFS/SFER_EDA10/", "GDFS/QPF_R24/", "GDFS/QPF_R12/", "GDFS/QPF_R06/", "GDFS/QPF_R03/",
    #         "GDFS/QPF_PPH/", "GDFS/OEFS_TMP/", "GDFS/OEFS_TMIN/", "GDFS/OEFS_TMAX/", "GDFS/OEFS_RRH/",
    #         "GDFS/OEFS_RHMX/", "GDFS/OEFS_RHMI/", "GDFS/OEFS_ECT/"]
    # for yb in ybll:
    #     getDataUrlNew.GetUrlAndDownloadsFileByType(yb, "5KM预报网格")
    # getDataUrlNew.GetUrlAndDownloadsFileByType("OCF/1H/", "OCF逐小时预报")
    # getDataUrlNew.GetUrlAndDownloadsFileByType("RADI_CHN_MUL_HOR/", "辐射逐小时")
    # getDataUrlNew.GetUrlAndDownloadsFileByType("SCW_CN/", "SCW强对流预报")
    #
    #
    # # tb_qxsj里面的数据处理之后存储进入tb_jrsj(应用服务器需要使用）
    # # 找到tb_qxsj未同步到tb_jrsj接口数据
    # qxsjsYb = QxsjToJrsj.GetQxsjByTypeNoTb("5KM预报网格")
    # qxsjsOCF = QxsjToJrsj.GetQxsjByTypeNoTb("OCF逐小时预报")
    # qxsjsRadi = QxsjToJrsj.GetQxsjByTypeNoTb("辐射逐小时")
    # qxsjsSCW = QxsjToJrsj.GetQxsjByTypeNoTb("SCW强对流预报")
    # if qxsjsYb is not None and len(qxsjsYb) != 0:
    #     # tb_qxsj处理之后存储到tb_jrsj
        # QxsjToJrsj.QxsjToJrsj5KMwg(qxsjsYb)
    #     # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
    #     QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsYb)
    # if qxsjsOCF is not None and len(qxsjsOCF) != 0:
    #     # tb_qxsj处理之后存储到tb_jrsj
    #     QxsjToJrsj.QxsjToJrsjOCF(qxsjsOCF)
    #     # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
    #     QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsOCF)
    # if qxsjsRadi is not None and len(qxsjsRadi) != 0:
    #     QxsjToJrsj.QxsjToJrsjCommon(qxsjsRadi)
    #     QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsRadi)
    # if qxsjsSCW is not None and len(qxsjsSCW) != 0:
    #     QxsjToJrsj.QxsjToJrsjCommon(qxsjsSCW)
    #     QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsSCW)

    # 5KM预报网格存储未定
    # 文件内容不解析直接存储到mysql中，存储非结构化的文本内容
    print('11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
    # jrsjSCW = FileDataToMysql.GetJrsjByTypeNoFileStore("SCW强对流预报")
    # print(jrsjSCW)
    # jrsjOCF = FileDataToMysql.GetJrsjByTypeNoFileStore("OCF逐小时预报")
    # jrsjRadi = FileDataToMysql.GetJrsjByTypeNoFileStore("辐射逐小时")
    # jrsjYbwgNCFile = FileDataToMysql.GetJrsjByTypeNoFileStore("5KM预报网格")
    # if jrsjSCW is not None and len(jrsjSCW) != 0:
        # FileDataToMysql.JrsjSCWHourFilesIntoMysql(jrsjSCW)
        # FileDataToMysql.SetJrsjFileStoreStatus1(jrsjSCW)
        # JxdataToMysql.JrsjSCWJxIntoMysql(jrsjSCW)
    #     JxdataToMysql.SetJrsjJxStatus1(jrsjSCW)
    # if jrsjOCF is not None and len(jrsjOCF) != 0:
    #     FileDataToMysql.JrsjOcfHourFilesIntoMysql(jrsjOCF)
    #     FileDataToMysql.SetJrsjFileStoreStatus1(jrsjOCF)
    # if jrsjRadi is not None and len(jrsjRadi) != 0:
    #     FileDataToMysql.JrsjRadiHourFilesIntoMysql(jrsjRadi)
    #     FileDataToMysql.SetJrsjFileStoreStatus1(jrsjRadi)
    # if len(jrsjYbwgNCFile) != 0:
        # FileDataToMysql.JrsjYbNCFilesIntoMysql(jrsjYbwgNCFile)
    #     FileDataToMysql.SetJrsjFileStoreStatus1(jrsjYbwgNCFile)

    # 此处放辐射追小时txt解析写入tb_radi
    jrsjOCF = JxdataToMysql.GetJrsjByTypeNoJxStore("OCF逐小时预报")
    if jrsjOCF is not None and len(jrsjOCF) != 0:
        JxdataToMysql.JrsjOCFJxIntoMysql(jrsjOCF)
    #     JxdataToMysql.SetJrsjJxStatus1(jrsjOCF)
    # # 此处放辐射追小时txt解析写入tb_radi
    # jrsjRadi = JxdataToMysql.GetJrsjByTypeNoJxStore("辐射逐小时")
    # if jrsjRadi is not None and len(jrsjRadi) != 0:
    #     JxdataToMysql.JrsjRadiJxIntoMysql(jrsjRadi)
    #     JxdataToMysql.SetJrsjJxStatus1(jrsjRadi)
        # 此处放辐射追小时txt解析写入tb_radi
    print('****************************************************************************************************')

#灾害预警信息-获取-存储-同步解析数据(供应用服务器应用）
def Zhyjxx():
    # #下载文件数据，接口写入tb_qxsj
    # getDataUrlNew.GetUrlAndDownloadsFileByType("alarm/", "灾害预警")

    # tb_qxsj里面的数据处理之后存储进入tb_jrsj(应用服务器需要使用）
    # 找到tb_qxsj未同步到tb_jrsj接口数据
    # qxsjsZhyj = QxsjToJrsj.GetQxsjByTypeNoTb("灾害预警")
    # if qxsjsZhyj is not None and len(qxsjsZhyj) != 0:
        # tb_qxsj处理之后存储到tb_jrsj
        # QxsjToJrsj.QxsjToJrsjAlarm(qxsjsZhyj)
        # 数据存储成之后将标志未更新为1 表示该数据已经同步到tb_jrsj中
        # QxsjToJrsj.SetQxsjJrsjStatus1(qxsjsZhyj)

    # 文件内容不解析直接存储到mysql中，存储非结构化的文本内容
    # jrsjAlarm = FileDataToMysql.GetJrsjByTypeNoFileStore("灾害预警")
    # if jrsjAlarm is not None and len(jrsjAlarm) != 0:
        # FileDataToMysql.JrsjAlarmFilesIntoMysql(jrsjAlarm)
        # FileDataToMysql.SetJrsjFileStoreStatus1(jrsjAlarm)

    # 将灾害预警的xml解析存储到tb_alarm表中（应用数据库中需要使用）
    # 获取未解析到tb_alarm中的接入数据信息（从tb_jrsj中读取，因为解析后的表中会涉及tb_jrsj的id作为外键）
    jrsjAlarm = JxdataToMysql.GetJrsjByTypeNoJxStore("灾害预警")
    if jrsjAlarm is not None and len(jrsjAlarm) != 0:
        # 解析数据存入mysql
        JxdataToMysql.JrsjAlarmJxIntoMysql(jrsjAlarm)
        # 解析处理之后的接入数据接口tb_jrsj,将标志位更新为1,
        # JxdataToMysql.SetJrsjJxStatus1(jrsjAlarm)

if __name__ == '__main__':
    # 气象环境实时数据-获取-存储-同步解析数据(供应用服务器应用）
    # QxhjSssj()
    # ncToMysql.main()
    # 气象灾害特征数据-获取-存储-同步解析数据(供应用服务器应用）
    # Qxzhtzsj()
    # 气象预报信息数据-获取-存储-同步解析数据(供应用服务器应用）
    # Qxybxxsj()
    # 灾害预警信息-获取-存储-同步解析数据(供应用服务器应用）
    Zhyjxx()




