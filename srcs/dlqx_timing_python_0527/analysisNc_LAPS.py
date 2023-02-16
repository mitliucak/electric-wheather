import jaydebeapi
from netCDF4 import Dataset
import numpy as np
import json
import datetime
import pymysql

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

def analysisNc(filePath,fileId,fileName):
    nc_obj = Dataset(filePath)
    # print('--------------------')
    # print(nc_obj.Originating_or_generating_Center)
    info_Center = nc_obj.Originating_or_generating_Center
    # print(nc_obj.Originating_or_generating_Subcenter)
    info_Subcenter = nc_obj.Originating_or_generating_Subcenter
    # print(nc_obj.GRIB_table_version)
    info_version = nc_obj.GRIB_table_version
    # print(nc_obj.Type_of_generating_process)
    info_process = nc_obj.Type_of_generating_process
    # print(nc_obj.Conventions)
    info_Conventions = nc_obj.Conventions
    # print(nc_obj.history)
    info_history = nc_obj.history
    # print(nc_obj.featureType)
    info_featureType = nc_obj.featureType
    # print(nc_obj.History)
    info_history_long = nc_obj.History
    # print(nc_obj.geospatial_lat_min)
    info_lat_min = nc_obj.geospatial_lat_min
    # print(nc_obj.geospatial_lat_max)
    info_lat_max = nc_obj.geospatial_lat_max
    # print(nc_obj.geospatial_lon_min)
    info_lon_min = nc_obj.geospatial_lon_min
    # print(nc_obj.geospatial_lon_max)
    info_lon_max = nc_obj.geospatial_lon_max
    # print(nc_obj.dimensions)
    # print(nc_obj.groups)
    # print('--------------------')

    #获取开始的时间 因为time里面存储是[24,48,...240]  而不是具体的时间
    sinceTimeStr = nc_obj.variables['time'].units.split(" ")[-1]
    sinceTime = datetime.datetime.strptime(sinceTimeStr, '%Y-%m-%dT%H:%M:%SZ')
    # print(sinceTime)
    # newtime = (sinceTime + datetime.timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
    # print(newtime)

    ###最直接的办法，获取每个变量的缩写名字，标准名字(long_name),units和shape大小。这样很方便后续操作
    dimension_time = ''
    dimension_y = ''
    dimension_x = ''
    dimension_height_above_ground = ''
    dimension_height_above_ground1 = ''
    for key in nc_obj.variables.keys():
        if key == 'time':
            dimension_time = nc_obj.variables[key].shape[0]
        if key == 'y':
            dimension_y = nc_obj.variables[key].shape[0]
        if key == 'x':
            dimension_x = nc_obj.variables[key].shape[0]
        if key == 'height_above_ground':
            dimension_height_above_ground = nc_obj.variables[key].shape[0]
        if key == 'height_above_ground1':
            dimension_height_above_ground1 = nc_obj.variables[key].shape[0]

    # #=============================连接数据库==================================
    try:
        sql = "INSERT INTO tb_nc_info_laps(jrsjid,file_name,info_Center,info_Subcenter,info_version,info_process,info_Conventions,info_history,info_featureType,info_history_long,info_lat_min,info_lat_max,info_lon_min,info_lon_max,dimension_time,dimension_y,dimension_x,dimension_height_above_ground,dimension_height_above_ground1) \
            VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') \
            " % (fileId,fileName,info_Center,info_Subcenter,info_version,info_process,info_Conventions,info_history,info_featureType,info_history_long,info_lat_min,info_lat_max,info_lon_min,info_lon_max,dimension_time,dimension_y,dimension_x,dimension_height_above_ground,dimension_height_above_ground1)
        # print(sql)
        try:
            # conn3 = pymysql.connect(host='localhost', user='root', passwd='password', port=3306, db='d3')  # 连接数据库
            # cursor3 = conn3.cursor()
            conn3 = jaydebeapi.connect(driver,jdbc_url3,uandp,jar_file)
            cursor3 = conn3.cursor()
            cursor3.execute(sql)
            #conn3.commit()

            # lastid = cursor3.lastrowid
            #print("lastid是：",lastid)
            print("tb_nc_info_laps save sql成功")
        except Exception as e:
            print('tb_nc_info_laps save ERROR!' + str(e))
        else:
            for key in nc_obj.variables.keys():
                long_name = ''
                units = ''
                description = ''
                missing_value = ''
                grid_mapping = ''
                coordinates = ''
                Grib_Statistical_Interval_Type = ''
                Grib_Variable_Id = ''
                Grib2_Parameter = ''
                Grib2_Parameter_Discipline = ''
                Grib2_Parameter_Category = ''
                Grib2_Parameter_Name = ''
                Grib2_Level_Type = ''
                Grib2_Generating_Process_Type = ''
                standard_name = ''
                calendar = ''
                _CoordinateAxisType = ''
                positive = ''
                Grib_level_type = ''
                datum = ''
                _CoordinateZisPositive = ''
                grid_mapping_name = ''
                latitude_of_projection_origin = ''
                longitude_of_central_meridian = ''
                standard_parallel = ''
                earth_radius = ''
                _CoordinateTransformType =''
                dimensions = ''
                # print(nc_obj.variables[key])
                # print('--------------------')
                # print(key)
                variable_name = key
                variableArr = []
                # print(nc_obj.variables[key].dimensions)
                dimensionsList = list(nc_obj.variables[key].dimensions)
                dimensions = ','.join(dimensionsList)
                # print(nc_obj.variables[key].shape)
                dd = nc_obj.variables[key]
                if hasattr(dd, 'long_name'):
                    # print(nc_obj.variables[key].long_name)
                    long_name = nc_obj.variables[key].long_name
                if hasattr(dd, 'units'):
                    # print(nc_obj.variables[key].units)
                    units = nc_obj.variables[key].units
                if hasattr(dd, 'description'):
                    # print(nc_obj.variables[key].description)
                    description = nc_obj.variables[key].description
                if hasattr(dd, 'missing_value'):
                    # print(nc_obj.variables[key].missing_value)
                    missing_value = nc_obj.variables[key].missing_value
                if hasattr(dd, 'grid_mapping'):
                    # print(nc_obj.variables[key].grid_mapping)
                    grid_mapping = nc_obj.variables[key].grid_mapping
                if hasattr(dd, 'coordinates'):
                    # print(nc_obj.variables[key].coordinates)
                    coordinates = nc_obj.variables[key].coordinates
                if hasattr(dd, 'Grib_Variable_Id'):
                    # print(nc_obj.variables[key].Grib_Variable_Id)
                    Grib_Variable_Id = nc_obj.variables[key].Grib_Variable_Id
                if hasattr(dd, 'Grib2_Parameter'):
                    # print(nc_obj.variables[key].Grib2_Parameter)
                    Grib2_Parameter = nc_obj.variables[key].Grib2_Parameter
                if hasattr(dd, 'Grib2_Parameter_Discipline'):
                    # print(nc_obj.variables[key].Grib2_Parameter_Discipline)
                    Grib2_Parameter_Discipline = nc_obj.variables[key].Grib2_Parameter_Discipline
                if hasattr(dd, 'Grib2_Parameter_Category'):
                    # print(nc_obj.variables[key].Grib2_Parameter_Category)
                    Grib2_Parameter_Category = nc_obj.variables[key].Grib2_Parameter_Category
                if hasattr(dd, 'Grib2_Parameter_Name'):
                    # print(nc_obj.variables[key].Grib2_Parameter_Name)
                    Grib2_Parameter_Name = nc_obj.variables[key].Grib2_Parameter_Name
                if hasattr(dd, 'Grib2_Level_Type'):
                    # print(nc_obj.variables[key].Grib2_Level_Type)
                    Grib2_Level_Type = nc_obj.variables[key].Grib2_Level_Type
                if hasattr(dd, 'Grib2_Generating_Process_Type'):
                    # print(nc_obj.variables[key].Grib2_Generating_Process_Type)
                    Grib2_Generating_Process_Type = nc_obj.variables[key].Grib2_Generating_Process_Type
                if hasattr(dd, 'standard_name'):
                    # print(nc_obj.variables[key].standard_name)
                    standard_name = nc_obj.variables[key].standard_name
                if hasattr(dd, 'calendar'):
                    # print(nc_obj.variables[key].calendar)
                    calendar = nc_obj.variables[key].calendar
                if hasattr(dd, '_CoordinateAxisType'):
                    # print(nc_obj.variables[key]._CoordinateAxisType)
                    _CoordinateAxisType = nc_obj.variables[key]._CoordinateAxisType
                if hasattr(dd, 'positive'):
                    # print(nc_obj.variables[key].positive)
                    positive = nc_obj.variables[key].positive
                if hasattr(dd, 'Grib_level_type'):
                    # print(nc_obj.variables[key].Grib_level_type)
                    Grib_level_type = nc_obj.variables[key].Grib_level_type
                if hasattr(dd, 'datum'):
                    # print(nc_obj.variables[key].datum)
                    datum = nc_obj.variables[key].datum
                if hasattr(dd, '_CoordinateZisPositive'):
                    # print(nc_obj.variables[key]._CoordinateZisPositive)
                    _CoordinateZisPositive = nc_obj.variables[key]._CoordinateZisPositive
                if hasattr(dd, 'grid_mapping_name'):
                    # print(nc_obj.variables[key].grid_mapping_name)
                    grid_mapping_name = nc_obj.variables[key].grid_mapping_name
                if hasattr(dd, 'latitude_of_projection_origin'):
                    # print(nc_obj.variables[key].latitude_of_projection_origin)
                    latitude_of_projection_origin = nc_obj.variables[key].latitude_of_projection_origin
                if hasattr(dd, 'longitude_of_central_meridian'):
                    # print(nc_obj.variables[key].longitude_of_central_meridian)
                    longitude_of_central_meridian = nc_obj.variables[key].longitude_of_central_meridian
                if hasattr(dd, 'standard_parallel'):
                    # print(nc_obj.variables[key].standard_parallel)
                    standard_parallel = nc_obj.variables[key].standard_parallel
                if hasattr(dd, 'earth_radius'):
                    # print(nc_obj.variables[key].earth_radius)
                    earth_radius = nc_obj.variables[key].earth_radius
                if hasattr(dd, '_CoordinateTransformType'):
                    # print(nc_obj.variables[key]._CoordinateTransformType)
                    _CoordinateTransformType = nc_obj.variables[key]._CoordinateTransformType
                # print('--------------------')

                #维度值
                if len(dimensionsList) == 1:
                    variableArr = nc_obj.variables[key][:].tolist()
                else:
                    variablesList = (nc_obj.variables[key][:])
                    lists = []          ## 空列表
                    if(len(dimensionsList) == 3):
                        for item in variablesList:
                            lists2 = []
                            for item2 in item:
                                lists3 = []
                                for item3 in item2:
                                    lists3.append(item3)
                                lists2.append(lists3)
                            lists.append(lists2)

                    if(len(dimensionsList) == 4):
                        for item in variablesList:
                            lists2 = []
                            for item2 in item:
                                lists3 = []
                                for item3 in item2:
                                    lists4 = []
                                    for item4 in item3:
                                        lists4.append(item4)
                                    lists3.append(lists4)
                                lists2.append(lists3)
                            lists.append(lists2)

                    variableArr = lists

                # #=============================连接数据库==================================
                try:
                    sql = "INSERT INTO tb_nc_variables_laps(nc_id,variable_name,long_name,units,description,missing_value,grid_mapping,coordinates,Grib_Variable_Id,Grib2_Parameter,Grib2_Parameter_Discipline,Grib2_Parameter_Category,Grib2_Parameter_Name,Grib2_Level_Type,Grib2_Generating_Process_Type,standard_name,calendar,_CoordinateAxisType,positive,Grib_level_type,datum,_CoordinateZisPositive,grid_mapping_name,latitude_of_projection_origin,longitude_of_central_meridian,standard_parallel,earth_radius,_CoordinateTransformType,dimensions,variableArr) \
                            VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') \
                            " % (lastid,variable_name,long_name,units,description,missing_value,grid_mapping,coordinates,Grib_Variable_Id,Grib2_Parameter,Grib2_Parameter_Discipline,Grib2_Parameter_Category,Grib2_Parameter_Name,Grib2_Level_Type,Grib2_Generating_Process_Type,standard_name,calendar,_CoordinateAxisType,positive,Grib_level_type,datum,_CoordinateZisPositive,grid_mapping_name,latitude_of_projection_origin,longitude_of_central_meridian,standard_parallel,earth_radius,_CoordinateTransformType,dimensions,variableArr)
                    # print(sql)
                    try:
                        cursor3.execute(sql)
                        # conn3.commit()
                    except:
                        print('tb_nc_variables_laps save ERROR!')
                except:
                    print('tb_nc_variables_laps SQL ERROR!')
        finally:
            cursor3.close()
            conn3.close()
    except:
        print('tb_nc_info_laps SQL ERROR!')


def main(filePath,fileId,fileName):
    analysisNc(filePath,fileId,fileName)
