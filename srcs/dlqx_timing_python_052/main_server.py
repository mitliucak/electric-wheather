from flask import Flask
from threading import Thread
from gevent import pywsgi
from tqdm import tqdm
from netCDF4 import Dataset
import datetime
import time
import os
import json
from enum import Enum
import GDFSNcToJson_OEFS_ETC
import GDFSNcToJson_OEFS_RHMI
import GDFSNcToJson_OEFS_RHMX
import GDFSNcToJson_OEFS_RRH
import GDFSNcToJson_OEFS_TMAX
import GDFSNcToJson_OEFS_TMIN
import GDFSNcToJson_OEFS_TMP
import GDFSNcToJson_QPF_PPH
import GDFSNcToJson_QPF_R03
import GDFSNcToJson_QPF_R06
import GDFSNcToJson_QPF_R12
import GDFSNcToJson_QPF_R24
import GDFSNcToJson_SFER_EDA10
import GDFSNcToJson_SHYS_VIS

TEST_LAPS_NC = "/data/history_data/LAPS/GR2/20220614/ShanXi_MSP3_PMSC_LAPS3KM_ME_L88_CHN_202206141500_00000-00000.GR2.nc"

app = Flask(__name__)

data_names = ['OEFS_ECT', 'OEFS_RHMI', 'OEFS_RHMX', 'OEFS_RRH', 'OEFS_TMAX', 'OEFS_TMIN',  'OEFS_TMP', 'QPF_PPH', 'QPF_R03', 'QPF_R06', 'QPF_R12', 'QPF_R24',  'SFER_EDA10',  'SHYS_VIS']

test_json = {"type": "FeatureCollection", "features": 
[{
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [118.06741141721142, 24.44374568949037]
        },
        "properties": {
            "value": 64
        }
    },
    {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [118.06995638756251, 24.443960213339437]
        },
        "properties": {
            "value": 120
        }
    },
    {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [118.06943796767592, 24.44177205295297]
        },
        "properties": {
            "value": 146
        }
    },
    {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [118.07221858343115, 24.442415633479683]
        },
        "properties": {
            "value": 98
        }
    },
    {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [118.07104035641697, 24.43885444668797]
        },
        "properties": {
            "value": 87
        }
    },
    {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [118.07132313089932, 24.43846828785817]
        },
        "properties": {
            "value": 45
        }
    },
    {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [118.07344393952684, 24.438167941283425]
        },
        "properties": {
            "value": 84
        }
    },
    {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [118.07372671401089, 24.44087103469434]
        },
        "properties": {
            "value": 84
        }
    }
]
}
class Atmosphere_type(Enum):
    rain = 1
    temperature = 2

data_paths = [os.path.join('/data/history_data/GDFS/', data_name) for data_name in data_names]


def get_geo_json(data_name):
    print("data name : {}".format(data_name))
    # 获取最新的数据
    path = os.path.join('/data/history_data/GDFS/', data_name)
    model_name = 'GDFSNcToJson_' + data_name
    sub_dir = '20211225'
    if not os.path.exists(os.path.join(path,sub_dir)):
        sub_dir = os.listdir(path)[-1]
    sub_dir = os.path.join(path, sub_dir)
    filename = os.listdir(sub_dir)[-1]
    print("nc file name : {}".format(filename))
    geo_json = eval(model_name).ReadFromNc(os.path.join(sub_dir, filename))
    return geo_json

# 坐标转换
def convert_lat_lon(min_max_data, yx):
    lat_min, lat_len, lon_min, lon_len, x_min, x_len, y_min, y_len = min_max_data
    y, x = yx
    lon = lon_min + lon_len * (y-y_min) / y_len
    lat = lat_min + lat_len * (x-x_min) / x_len
    return [lon, lat]



# 获取实时数据
def get_laps_data(filePath, data_type):
    start = time.time()
    nc_obj = Dataset(filePath)
    x = (nc_obj.variables['x'][:])
    y = (nc_obj.variables['y'][:])
    #time = (nc_obj.variables['time'][:])
    min_max_data = []
    lat_min = nc_obj.geospatial_lat_min
    lat_max = nc_obj.geospatial_lat_max
    lat_len = lat_max - lat_min
    min_max_data.append(lat_min)
    min_max_data.append(lat_len)
    lon_min = nc_obj.geospatial_lon_min
    lon_max = nc_obj.geospatial_lon_max
    lon_len = lon_max - lon_min
    min_max_data.append(lon_min)
    min_max_data.append(lon_len)
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
    x_min = min(x)
    x_max = max(x)
    x_len = x_max - x_min
    min_max_data.append(x_min)
    min_max_data.append(x_len)
    y_min = min(y)
    y_max = max(y)
    y_len = y_max - y_min
    min_max_data.append(y_min)
    min_max_data.append(y_len)
    #t = time.shape[0]
    #for i in range(t):
    #    Time = (sinceTime + datetime.timedelta(hours=time[i])).strftime("%Y-%m-%d %H:%M:%S")
    #    print(Time)
    rain_json = {"type": "FeatureCollection", "features": []}
    temperature_json = {"type": "FeatureCollection", "features": []}
    k = 0
    for i in range(m):
        for j in range(n):
            #Time = (sinceTime + datetime.timedelta(hours=time[k])).strftime("%Y-%m-%d %H:%M:%S")
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

            feature = {"type": "Feature", "geometry":{"type": "Point",}, "propertity": {}}
            feature["geometry"]["coordinates"] = convert_lat_lon(min_max_data,[round(float(YCoordinate), 7), round(float(XCoordinate), 7)])
            # set rain fall value
            feature["propertity"]["value"] = round(float(JslYxs), 7)
            rain_json["features"].append(feature)
            # set temperature
            feature["propertity"]["value"] = round(float(LDTem), 7)
            temperature_json["features"].append(feature)
    end = time.time()
    print("time use : {}".format(end-start))
    if data_type == Atmosphere_type.rain:
        return json.dumps(rain_json)
    elif data_type == Atmosphere_type.temperature:
        return json.dumps(temperature_json)
    else:
        return json.dumps(rain_json)
    


@app.route('/get_temperature_realtime')
def get_temperature_realtime():
    return get_laps_data(TEST_LAPS_NC, Atmosphere_type.temperature)


@app.route('/get_rainfall_realtime')
def get_rainfall_realtime(): 
    return get_laps_data(TEST_LAPS_NC, Atmosphere_type.rain)
    

if __name__ == '__main__':
    #server = pywsgi.WSGIServer(('0.0.0.0', 9093), app)
    #server.serve_forever()
    get_laps_data(TEST_LAPS_NC, Atmosphere_type.temperature)
