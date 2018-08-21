#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/..; pwd -P`
echo "bin dir: ${BIN_DIR}"

source ${BIN_DIR}/util/util.sh

run sample.NaturalSampleGenerator --day $day
run sample.NaturalRankSampleGenerator --day $day
run predict.ResultPredictor --day $day
run predict.ResultMerger --day $day

RERANK_PATH="/user/h_misearch/appmarket/rank/predict/download_history/date=$day"
OUTPUT_HDFS_PATH="/user/h_misearch/appmarket/pipeline_data/app_ctr"
OUTPUT_LOCAL_PATH="${BIN_DIR}/data"
C3_CLUSTER="--cluster c3prc-hadoop"
HADOOP_HOME="/home/work/tars/infra-client/bin/hadoop"

# Fetch test data
${HADOOP_HOME} ${C3_CLUSTER} fs -test -e ${RERANK_PATH}/_SUCCESS
if [ $? -eq 0 ] ;then
    ${HADOOP_HOME} ${C3_CLUSTER} fs -cat ${RERANK_PATH}/* > ${OUTPUT_LOCAL_PATH}/download_rerank.txt
    ${HADOOP_HOME} ${C3_CLUSTER} fs -copyFromLocal -f ${OUTPUT_LOCAL_PATH}/download_rerank.txt ${OUTPUT_HDFS_PATH}/download_rerank.txt
    echo "Generate download rerank data success."
else
    echo "Error! ${RERANK_PATH} is not exists."
fi

