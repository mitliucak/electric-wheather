#!/bin/sh
db=$1
if [ $db == '6' ]
then
    mysql -h127.0.0.1 -P13306 -uroot -p123456 -Ddlqxsync
elif [ $db == '7' ]
then
    mysql -h127.0.0.1 -P13307 -uroot -p123456 -Ddlqxsync
elif [ $db == '8' ]
then
    mysql -h127.0.0.1 -P13308 -uroot -p123456 -Ddlqx_08
else
    echo "the database is error"
fi
