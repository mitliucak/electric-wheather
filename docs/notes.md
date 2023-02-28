
# 说明文档

## 名词解释

dlqx：电力气象



## 数据库：
tb_qxsj: 存储爬取的原始数据
tb_jrsj: 存储解析出来的字段(应用服务器使用)

JDBC: jdbc:mysql://111.198.60.33:13308/08dlqx_zl?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=false&serverTimezone=GMT%2B8&autoReconnect=true
JDBC: jdbc:mysql://127.0.0.1:13308/08dlqx_zl?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=false&serverTimezone=GMT%2B8&autoReconnect=true

08是后台调用的，06和07同步数据中台用,06是解析的数据和文件信息都有，07只有数据，08是数据数据和用户表等
气象数据分为实况、预报、历史数据, 三种数据都需要持久化到06、07、08三个数据库。


## 资源：
机器：


## 数据文件和代码
气象数据分为实况、预报、历史数据, 三种数据都需要持久化到06、07、08三个数据库。
实况数据
一个nc文件包含 未来十天的预报数据，每三个小时一块数据，一块数据包含123X87=10,701个坐标点的预报信息


## 产品



## 代码

## gis知识
1.gis坐标系有两类，
第一类是地理坐标系统，第二类是投影坐标系统。地理坐标系统中，通过经纬度（单位是角度）来标识位置，也就是解析立体几何中的球面坐标体系；投影坐标系统中，我们使用笛卡尔积中的x，y（米、千米为单位）来表达标识点。
存在两类系统的原因，投影坐标系统主要是为了解决几何计算（比如面积、长度）上面的麻烦。我们一般使用地理坐标系统（xx度东经，yy度北纬）标识位置，我们使用投影坐标系统，计算某个区域的面积。一个擅长定位，一个擅长计算。

2.常见坐标体系
为了管理不同的坐标系统，EPSG给这些坐标系统定义了不同id，id名称叫做WKID（We Known ID）。
常见地理坐标系统ID：
WSG84：WKID=4326，google地图、osm地图、加密前的高德、百度也是用的这个系统，应用极其广泛，
CGCS2000： WKID=4490，中国北斗导航系统使用的是这个。中文名称叫做中国大地坐标体系2000
北京54、西安80：WKID分别为4214、4610，已经逐渐不再使用，但是很多历史数据保存的是这个，需要尽快转到CGCS2000上面来。

常见投影坐标系统
投影的方法一般有投影到圆柱面、圆锥面

3.常见名词解释
lat：latitude 纬度（横着的）中国一般是3、4十左右
lon：longitude 经度（竖着的）中国一般是一百多

reference：http://www.360doc.com/content/22/1020/12/29346318_1052458434.shtml
lambert_conformal_conic: https://en.wikipedia.org/wiki/Lambert_conformal_conic_projection
