#!/usr/bin/env bash

BIN_DIR=`cd $(dirname $0); pwd -P`
echo "bin dir: ${BIN_DIR}"

nohup /usr/java/openjdk1.8.0_202/bin/java -Xms512m -Xmx1024m -cp misearch-rank-music.jar \
    -Djava.security.krb5.conf=/etc/krb5-hadoop.conf \
    -Dhadoop.property.hadoop.security.authentication=kerberos \
    -Dhadoop.property.hadoop.client.keytab.file=/etc/h_misearch.keytab \
    -Dhadoop.property.hadoop.client.kerberos.principal=h_misearch@XIAOMI.HADOOP \
    com.xiaomi.misearch.rank.music.prepare.QQRankFetcher \
    > fetch_qq_rank.log 2>&1 &
