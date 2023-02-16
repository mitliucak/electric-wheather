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

# conn = pymysql.connect(host= 'localhost',db='dlqxsync',user='root',password='123456',charset='utf8',port=13307)
# cursor=conn.cursor()
jar_file = 'mysql-connector-java-8.0.12.jar'
driver = 'com.mysql.cj.jdbc.Driver'
jdbc_url1 = 'jdbc:mysql://121.52.212.109:13306/06dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
jdbc_url2 = 'jdbc:mysql://121.52.212.109:13307/07dlqxsync?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
jdbc_url3 = 'jdbc:mysql://121.52.212.109:13308/08dlqx_zl?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true'
uandp = ["root", "123456"]

#从nc中获取某一个属性的全部值，返回装有json字典对象的List
# 传入值filePath：nc文件路径
def ReadFromNc(filePath,jrsjid1,jrsjid2,jrsjid3):
    try:
        sql3 = "UPDATE tb_jrsj SET jrsj_jxstorestatus = 1 AND jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (jrsjid1)
        sql4 = "UPDATE tb_jrsj SET jrsj_jxstorestatus = 1 AND jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (jrsjid2)
        sql5 = "UPDATE tb_jrsj SET jrsj_jxstorestatus = 1 AND jrsj_filestorestatus = 1 WHERE jrsj_id = %s " % (jrsjid3)

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
            nc_obj = Dataset(filePath)
            # print('--------------------')
            print("----------nc_obj-----------------",nc_obj)
            # print('--------------------')
            x = (nc_obj.variables['x'][:])
            y = (nc_obj.variables['y'][:])
            time = (nc_obj.variables['time'][:])
            # 露点温度
            Dewpoint_temperature_height_above_ground = (nc_obj.variables['Dewpoint_temperature_height_above_ground'][:])
            # 相对湿度
            Relative_humidity_height_above_ground = (nc_obj.variables['Relative_humidity_height_above_ground'][:])
            # 温度
            Temperature_height_above_ground = (nc_obj.variables['Temperature_height_above_ground'][:])
            # 1小时累计降水量
            Total_precipitation_surface = (nc_obj.variables['Total_precipitation_surface'][:])
            # 2分钟平均风向
            Wind_direction_from_which_blowing_height_above_ground = (nc_obj.variables['Wind_direction_from_which_blowing_height_above_ground'][:])
            # 2分钟平均风速
            Wind_speed_height_above_ground = (nc_obj.variables['Wind_speed_height_above_ground'][:])
            # U风分量
            u_component_of_wind_height_above_ground = (nc_obj.variables['u-component_of_wind_height_above_ground'][:])
            # v风分量
            v_component_of_wind_height_above_ground = (nc_obj.variables['v-component_of_wind_height_above_ground'][:])
            
            #获取开始的时间 因为time里面存储是[24,48,...240]  而不是具体的时间
            sinceTimeStr = nc_obj.variables['time'].units.split(" ")[-1]
            sinceTime = datetime.datetime.strptime(sinceTimeStr, '%Y-%m-%dT%H:%M:%SZ')
            # print(sinceTime)

            jsondictList = []
            m = x.shape[0]  # 94
            n = y.shape[0]  # 83
            t = time.shape[0]
            tup = []
            for k in range(t):
                for i in range(m):
                    for j in range(n):
                        Time = (sinceTime + datetime.timedelta(hours=time[k])).strftime("%Y-%m-%d %H:%M:%S")
                        # X坐标值
                        XCoordinate = x[i]
                        # Y坐标值
                        YCoordinate = y[j]
                        # 露点温度
                        LDTem = Dewpoint_temperature_height_above_ground[k, 0, j, i]
                        # 相对湿度
                        Xdsd = Relative_humidity_height_above_ground[k, 0, j, i]
                        # 温度
                        Tem = Temperature_height_above_ground[k, 0, j, i]
                        # 1小时累计降水量
                        JslYxs = Total_precipitation_surface[k, j, i]
                        # 2分钟平均风向
                        WindDirect = Wind_direction_from_which_blowing_height_above_ground[k, 0, j, i]
                        # 2分钟平均风速
                        WindSpeed = Wind_speed_height_above_ground[k, 0, j, i]
                        # U风分量
                        UCom = u_component_of_wind_height_above_ground[k, 0, j, i]
                        # v风分量
                        VCom = v_component_of_wind_height_above_ground[k, 0, j, i]

                        # print("x:"+str(XCoordinate)
                        #       +" y:"+ str(YCoordinate)
                        #       +" 露点温度："+str(LDTem)
                        #       +" 相对湿度："+str(Xdsd)
                        #       +" 温度："+str(Tem)
                        #       +" 1小时累计降水量："+str(JslYxs)
                        #       +" 2分钟平均风向："+str(WindDirect)
                        #       +" 2分钟平均风速："+str(WindSpeed)
                        #       +" U风分量："+ str(UCom)
                        #       +" V风分量："+ str(VCom))

                        NcOneObjectDict = dict()
                        NcOneObjectDict['XCoordinate'] = round(float(XCoordinate), 7)
                        NcOneObjectDict['YCoordinate'] = round(float(YCoordinate), 7)
                        NcOneObjectDict['LDTem'] = round(float(LDTem), 7)
                        NcOneObjectDict['Xdsd'] = round(float(Xdsd), 7)
                        NcOneObjectDict['Tem'] = round(float(Tem), 7)
                        NcOneObjectDict['JslYxs'] = round(float(JslYxs), 7)
                        NcOneObjectDict['WindDirect'] = round(float(WindDirect), 7)
                        NcOneObjectDict['WindSpeed'] = round(float(WindSpeed), 7)
                        NcOneObjectDict['UCom'] = round(float(UCom), 7)
                        NcOneObjectDict['VCom'] = round(float(VCom), 7)
                        jsondictList.append(NcOneObjectDict);

                        # #=============================连接数据库==================================
                        # sql1 = "INSERT INTO tb_laps(laps_xcoordinate,laps_ycoordinate,laps_ldtem,laps_xdsd,laps_tem,laps_jslyxs,laps_winddirect,laps_windspeed,laps_ucom,laps_vcom,laps_time,laps_jrsjid) \
                                # VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                                # (XCoordinate,YCoordinate,LDTem,Xdsd,Tem,JslYxs,WindDirect,WindSpeed,UCom,VCom,Time,jrsjid1)
                        # sql2 = "INSERT INTO tb_laps(laps_xcoordinate,laps_ycoordinate,laps_ldtem,laps_xdsd,laps_tem,laps_jslyxs,laps_winddirect,laps_windspeed,laps_ucom,laps_vcom,laps_time,laps_jrsjid) \
                                # VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                                # (XCoordinate,YCoordinate,LDTem,Xdsd,Tem,JslYxs,WindDirect,WindSpeed,UCom,VCom,Time,jrsjid1)






                        sql1 = "INSERT INTO tb_laps(laps_xcoordinate,laps_ycoordinate,laps_ldtem,laps_xdsd,laps_tem,laps_jslyxs,laps_winddirect,laps_windspeed,laps_ucom,laps_vcom,laps_time,laps_jrsjid) \
                                VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                                (XCoordinate,YCoordinate,LDTem,Xdsd,Tem,JslYxs,WindDirect,WindSpeed,UCom,VCom,Time,jrsjid1)
                        sql2 = "INSERT INTO tb_laps(laps_xcoordinate,laps_ycoordinate,laps_ldtem,laps_xdsd,laps_tem,laps_jslyxs,laps_winddirect,laps_windspeed,laps_ucom,laps_vcom,laps_time,laps_jrsjid) \
                                VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                                (XCoordinate,YCoordinate,LDTem,Xdsd,Tem,JslYxs,WindDirect,WindSpeed,UCom,VCom,Time,jrsjid2)

                        try:
                            cursor1.execute(sql1)
                            print("-----------d1数据库tb_laps表插入成功--------")
                            # conn1.commit()
                        except Exception as e:
                            print("LAPSNcToJson(1) ----- ")

                        try:
                            cursor2.execute(sql2)
                            print("-----------d2数据库tb_laps表插入成功--------")
                            # conn1.commit()
                            # conn2.commit()
                        except Exception as e:
                            print("LAPSNcToJson(2) ----- ")

            try:
                cursor1.execute(sql3)
                print("更新tb_jrsj表，标志位为1")
                # conn1.commit()
            except Exception as e:
                print("LAPSNcToJson(3) ----- ",e)
                
            try:
                cursor2.execute(sql4)
                # conn2.commit()
            except Exception as e:
                print("LAPSNcToJson(4) ----- ",e)

            try:
                cursor3.execute(sql5)
                # conn3.commit()
            except Exception as e:
                print("LAPSNcToJson(6) ----- ",e)

            return jsondictList

        except Exception as e:
            print("LAPSNcToJson 文件解析异常 ----- ")

    except Exception as e:
        print("LAPSNcToJson 数据库连接异常 ----- ")
    finally:
        # cursor.close()
        # conn.close()
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        cursor3.close()
        conn3.close()


def main(filePath,jrsjid1,jrsjid2,jrsjid3):
    jsondictList = ReadFromNc(filePath,jrsjid1,jrsjid2,jrsjid3)
