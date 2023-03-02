TODO: 2023-2-21

数据流转过程:
通过爬虫爬取的数据先进入到 气象数据抓取
1.实况数据提供实时接口


2.预报数据入库
历史数据写入到tb_qxsj中，把tb_qxsj放入到tb_jrsj, 通过tb_jrsj获取未解析数据，解析数据，入库
GDFS OCF RADI_CHN_MUL_HOR SCW_CN


3.历史数据入库
CG_QXZH
HJ_QXZH
DX_JBZH

4.实时数据入库
SURF_CHN_MUL_HOR_N
SURF_CHN_MUL_HOR
LAPS/GR2


5.预警数据入库
alarm

