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

WEEK_DAY=`date -d "${day}" +%w`
echo "week day: ${WEEK_DAY}"
if [ ${WEEK_DAY} -eq 2 ] || [ ${WEEK_DAY} -eq 4 ] || [ ${WEEK_DAY} -eq 6 ]; then
    ${JAVA8_HOME}/bin/java \
        -Xms512m -Xmx1g -cp appsearch-rank.jar ${KBS_OPTS} \
        com.xiaomi.misearch.appsearch.rank.lamdarank.SearchResultFetcher
fi

# Predict
run sample.NaturalSampleGenerator --day ${day}
run sample.NaturalRankSampleGenerator --day ${day}
run predict.ResultPredictor --day ${day}
run predict.ResultMerger --day ${day}

# Merge lamdarank result with download history, reserve top10 apps in lamdarank
${JAVA8_HOME}/bin/java \
    -Xms512m -Xmx2g -cp appsearch-rank.jar ${KBS_OPTS} \
    com.xiaomi.misearch.appsearch.rank.lamdarank.SearchRankMerger ${day}

# Evaluate
run predict.ResultEvaluator --day ${day} --type unified
run predict.ResultEvaluator --day ${day} --type merged

# Sync to Download History directory
export HADOOP_OPTS="-Dhadoop.property.hadoop.client.keytab.file=/etc/h_misearch.keytab \
                    -Dhadoop.property.hadoop.client.kerberos.principal=h_misearch@XIAOMI.HADOOP"

if [ -z "${HADOOP_HOME}" ]; then
    HADOOP_HOME="/home/work/tars/infra-client"
fi
HADOOP="${HADOOP_HOME}/bin/hadoop"
ZJY_CLUSTER="--cluster zjyprc-hadoop"
C3_CLUSTER="--cluster c3prc-hadoop"

OUTPUT_RANK_DIR=/user/h_misearch/appmarket/pipeline_data/app_ctr/
RANK_UNIFIED_PATH=/user/h_misearch/appmarket/rank/predict/rank_unified/date=${day}
RANK_MERGED_FILE=/user/h_misearch/appmarket/rank/predict/rank_merged/date=${day}/rank_merged.txt

rm rank_unified.txt rank_merged.txt
if (${HADOOP} ${C3_CLUSTER} fs -ls "${RANK_UNIFIED_PATH}/_SUCCESS"); then
    ${HADOOP} ${C3_CLUSTER} fs -getmerge ${RANK_UNIFIED_PATH} rank_unified.txt
    ${HADOOP} ${ZJY_CLUSTER} fs -put -f rank_unified.txt ${OUTPUT_RANK_DIR}/rank_unified.txt
    echo "upload rank_unified.txt success"
fi

${HADOOP} ${C3_CLUSTER} fs -get ${RANK_MERGED_FILE} rank_merged.txt
${HADOOP} ${ZJY_CLUSTER} fs -put -f rank_merged.txt ${OUTPUT_RANK_DIR}/rank_merged.txt
echo "upload rank_merged.txt success"