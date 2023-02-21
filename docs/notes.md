
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
产品测试地址: http://111.198.60.33:8083/dist/index.html#/



## 代码

