#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/..; pwd -P`
echo "bin dir: ${BIN_DIR}"

source ${BIN_DIR}/util/util.sh

# JAVA8_HOME
if [ -z "${JAVA8_HOME}" ]; then
    JAVA8_HOME=/usr/java/jdk1.8.0_144
fi

echo ${JAVA8_HOME}

# Prepare query data
KBS_OPTS="-Djava.security.krb5.conf=/etc/krb5-hadoop.conf \
          -Dhadoop.property.hadoop.security.authentication=kerberos \
          -Dhadoop.property.hadoop.client.keytab.file=/etc/h_misearch.keytab \
          -Dhadoop.property.hadoop.client.kerberos.principal=h_misearch@XIAOMI.HADOOP"

${JAVA8_HOME}/bin/java \
    -Xms512m -Xmx1g -cp appsearch-rank.jar ${KBS_OPTS} \
    com.xiaomi.misearch.appsearch.rank.lamdarank.SearchResultFetcher

# Predict
run sample.NaturalSampleGenerator --day ${day}
run sample.NaturalRankSampleGenerator --day ${day}
run predict.ResultPredictor --day ${day}
run predict.ResultMerger --day ${day}

# Merge lamdarank result with download history, reserve top10 apps in lamdarank
${JAVA8_HOME}/bin/java \
    -Xms512m -Xmx2g -cp appsearch-rank.jar ${KBS_OPTS} \
    com.xiaomi.misearch.appsearch.rank.lamdarank.SearchRankMerger ${day}
