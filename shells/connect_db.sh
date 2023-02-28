#!/bin/sh
db=$1
mysql -h127.0.0.1 -P1330${db} -uroot -p123456 -Ddlqx_0${db}
