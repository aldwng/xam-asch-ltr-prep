#!/bin/bash

if [ $# != 1 ]; then
  day=`date --date="1 days ago" "+%Y%m%d"`
else
  day=$1
fi
KBS_OPTS="-Djava.security.krb5.conf=/etc/krb5-hadoop.conf -Dhadoop.property.hadoop.security.authentication=kerberos -Dhadoop.property.hadoop.client.keytab.file=/etc/h_misearch.keytab -Dhadoop.property.hadoop.client.kerberos.principal=h_misearch@XIAOMI.HADOOP"
${JAVA8_HOME}/bin/java -Xms512m -Xmx2g -cp appsearch-rank.jar $KBS_OPTS com.xiaomi.misearch.appsearch.rank.NaturalGenerator "${day}"