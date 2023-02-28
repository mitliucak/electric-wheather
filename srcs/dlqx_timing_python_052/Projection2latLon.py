import math
from netCDF4 import Dataset



# 兰伯特投影转换成大地坐标系
# reference: https://en.wikipedia.org/wiki/Lambert_conformal_conic_projection#Transformation
# the params
# lat : 经度
# lon : 纬度
# lat_ref : 参考经度
# lon_ref : 参考纬度
# R : 地球半径
# sp1, sp2: 标准并行参数

class LambertConformalConic():

    def __init__(nc_file):


def get_Lambert_conformal_conic_params(nc_file):
    data = Dataset(nc_file)
    lp = data.variables['LambertConformal_Projection']
    sp1, sp2 = lp.standard_parallel
    R = lp.earth_radius
    lat_ref, lon_ref = lp.latitude_of_projection_origin, lp.longitude_of_central_meridian
    

def get_n(sp1, sp2):
    n = math.log(math.cos(sp1) /math.cos(sp2)) / math.log(tan(0.25 * math.pi + 0.5 * sp2 ) / tan(0.25 * math.pi + 0.5 * sp1))
    return n


def get_F(sp1, n):
    F = math.cos(sp1) * math.pow( 0.25*math.pi + 0.5*sp1, n) / n




if __name__ == "__main__":
    
