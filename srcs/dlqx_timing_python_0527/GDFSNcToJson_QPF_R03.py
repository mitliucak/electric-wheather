from netCDF4 import Dataset
import json
import datetime
import pymysql
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

#从nc中获取某一个属性的全部值，返回装有json字典对象的List
# 传入值filePath：nc文件路径  Attr 需要解析的属性的名称
def ReadFromNc(filePath):
    nc_obj = Dataset(filePath)
    # print('--------------------')
    # print(nc_obj)
    # print('--------------------')
    #纬度
    lat = (nc_obj.variables['lat'][:])
    #经度
    lon = (nc_obj.variables['lon'][:])
    #时间（10天，维度为10）
    time = (nc_obj.variables['time'][:])
    #3小时降水
    Total_precipitation_surface_3_Hour_Accumulation = (nc_obj.variables['Total_precipitation_surface_3_Hour_Accumulation'][:])
    # 需要解析的属性变量在下标为0的位置,属性代表24小时降水，温度等等
    listKeys = list(nc_obj.variables.keys())
    Attr = (nc_obj.variables[listKeys[0]])

    #获取开始的时间 因为time里面存储是[24,48,...240]  而不是具体的时间
    sinceTimeStr = nc_obj.variables['time'].units.split(" ")[-1]
    sinceTime = datetime.datetime.strptime(sinceTimeStr, '%Y-%m-%dT%H:%M:%SZ')
    # print(sinceTime)
    # newtime = (sinceTime + datetime.timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
    # print(newtime)
    ncdatadict = dict()
    ncdatadict['lat'] = lat
    ncdatadict['lon'] = lon
    ncdatadict['time'] = time
    ncdatadict['Attr'] = Attr
    ncdatadict['sinceTime'] = sinceTime
    ncdatadict['Total_precipitation_surface_3_Hour_Accumulation'] = Total_precipitation_surface_3_Hour_Accumulation
    return ncdatadict

def NcToJson(ncdatadict, jrsjid1, jrsjid2, jrsjid3, AttrName):
    try:
        sql4 = "UPDATE tb_jrsj SET jrsj_jxstorestatus = 1 , jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (jrsjid1)
        sql5 = "UPDATE tb_jrsj SET jrsj_jxstorestatus = 1 , jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (jrsjid2)
        sql6 = "UPDATE tb_jrsj SET jrsj_jxstorestatus = 1 , jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (jrsjid3)

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

        try:
            Lat = ncdatadict['lat']
            Lon = ncdatadict['lon']
            Time = ncdatadict['time']
            Attr = ncdatadict['Attr']
            sinceTime = ncdatadict['sinceTime']
            Total_precipitation_surface_3_Hour_Accumulation = ncdatadict['Total_precipitation_surface_3_Hour_Accumulation']

            # print(Attr.shape)

            jsondictList = []
            TimeDimension = Attr.shape[0]
            LatDimension = Attr.shape[1]
            LonDimension = Attr.shape[2]
            for i in range(TimeDimension):
                for j in range(LatDimension):
                    for k in range(LonDimension):
                        time = (sinceTime + datetime.timedelta(hours=Time[i])).strftime("%Y-%m-%d %H:%M:%S")
                        #time = Time[i]
                        lat = Lat[j]
                        lon = Lon[k]
                        precipitation_3_Hour = Total_precipitation_surface_3_Hour_Accumulation[i, j, k]
                        attr = Attr[i, j, k]

                        NcOneObjectDict = dict()
                        NcOneObjectDict['time'] = time
                        NcOneObjectDict['lat'] = round(float(lat), 6)
                        NcOneObjectDict['lon'] = round(float(lon), 6)
                        NcOneObjectDict['precipitation_3_Hour'] = round(float(precipitation_3_Hour), 6)
                        NcOneObjectDict[AttrName] = 0
                        #NcOneObjectDict[AttrName] = attr
                        jsondictList.append(NcOneObjectDict)

                        # #=============================连接数据库==================================
                        sql1 = "INSERT INTO tb_gdfs_precipitation_3_hour(gdfs_precipitation_3_Hour_time,gdfs_precipitation_3_Hour_lat,gdfs_precipitation_3_Hour_lon,gdfs_precipitation_3_Hour_value,gdfs_precipitation_3_Hour_jrsjid) \
                                VALUES ('%s','%s','%s','%s','%s')" % (time, lat,lon,precipitation_3_Hour,jrsjid1)
                        sql2 = "INSERT INTO tb_gdfs_precipitation_3_hour(gdfs_precipitation_3_Hour_time,gdfs_precipitation_3_Hour_lat,gdfs_precipitation_3_Hour_lon,gdfs_precipitation_3_Hour_value,gdfs_precipitation_3_Hour_jrsjid) \
                                VALUES ('%s','%s','%s','%s','%s')" % (time,lat,lon,precipitation_3_Hour,jrsjid2)
                        sql3 = "INSERT INTO tb_gdfs_precipitation_3_hour(gdfs_precipitation_3_Hour_time,gdfs_precipitation_3_Hour_lat,gdfs_precipitation_3_Hour_lon,gdfs_precipitation_3_Hour_value,gdfs_precipitation_3_Hour_jrsjid) \
                                VALUES ('%s','%s','%s','%s','%s')" % (time,lat,lon,precipitation_3_Hour,jrsjid3)

                        try:
                            cursor1.execute(sql1)
                            print('tb_gdfs_precipitation_3_Hour存入成功')
                            print('111111111111111111111111111111111111111111111111111111111111111111111111111')
                            # conn1.commit()
                        except Exception as e:
                            print("GDFSNcToJson_QPF_R03(1) ----- ",e)

                        try:
                            cursor2.execute(sql2)
                            print('tb_gdfs_precipitation_3_Hour存入成功')
                            print('22222222222222222222222222222222222222222222222222222222222222222222222222')
                            # conn2.commit()
                        except Exception as e:
                            print("GDFSNcToJson_QPF_R03(2) ----- ",e)

                        try:
                            cursor3.execute(sql3)
                            print('tb_gdfs_precipitation_3_Hour存入成功')
                            print('3333333333333333333333333333333333333333333333333333333333333333333333333333')
                            # conn3.commit()
                        except Exception as e:
                            print("GDFSNcToJson_QPF_R03(3) ----- ",e)
            
            try:
                cursor1.execute(sql4)
                # conn1.commit()
            except Exception as e:
                print("GDFSNcToJson_QPF_R03(4) ----- ")
                
            try:
                cursor2.execute(sql5)
                # conn2.commit()
            except Exception as e:
                print("GDFSNcToJson_QPF_R03(5) ----- ")
                
            try:
                cursor3.execute(sql6)
                # conn3.commit()
            except Exception as e:
                print("GDFSNcToJson_QPF_R03(6) ----- ")

            return jsondictList

        except Exception as e:
            print("GDFSNcToJson_QPF_R03 文件解析异常 ----- ")

    except Exception as e:
        print("GDFSNcToJson_QPF_R03 数据库连接异常 ----- ")
    finally:
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


def main(filePath,jrsjid1,jrsjid2,jrsjid3):
    ncdatadict = ReadFromNc(filePath)
    #Js24代表过去24小时降水
    jsondictList = NcToJson(ncdatadict,jrsjid1,jrsjid2,jrsjid3,'Js24')

