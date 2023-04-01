#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/../..; pwd -P`
echo "bin dir: ${BIN_DIR}"

if [ $# != 1 ]; then
  day=`date --date="1 days ago" "+%Y%m%d"`
else
  day=$1
fi

echo "model data: ${day}"


export HADOOP_OPTS="-Dhadoop.property.hadoop.client.keytab.file=/etc/h_ms.keytab \
                    -Dhadoop.property.hadoop.client.kerberos.principal=h_ms@XMH"

if [ -z "${HADOOP_HOME}" ]; then
    HADOOP_HOME="/home/work/tars/infra-client"
fi
HADOOP="${HADOOP_HOME}/bin/hadoop"
ZJY_CLUSTER="--cluster zjyprc-hadoop"

SAMPLE_PATH_PREFIX="/user/h_ms/xam/lambdamart/sample/date=${day}"
TRAIN_PATH=${SAMPLE_PATH_PREFIX}/train
TEST_PATH=${SAMPLE_PATH_PREFIX}/test

OUTPUT_PATH="${BIN_DIR}/data"
rm -rf ${OUTPUT_PATH}
mkdir ${OUTPUT_PATH}

# Fetch train data
${HADOOP} ${ZJY_CLUSTER} fs -test -e ${TRAIN_PATH}/_SUCCESS
if [ $? -eq 0 ] ;then
    ${HADOOP} ${ZJY_CLUSTER} fs -cat ${TRAIN_PATH}/* > ${OUTPUT_PATH}/train.txt
    echo "Fetch train data success."
else 
    echo "Error! ${TRAIN_PATH} is not exists."
fi 

# Fetch test data
${HADOOP} ${ZJY_CLUSTER} fs -test -e ${TEST_PATH}/_SUCCESS
if [ $? -eq 0 ] ;then
    ${HADOOP} ${ZJY_CLUSTER} fs -cat ${TEST_PATH}/* > ${OUTPUT_PATH}/test.txt
    echo "Fetch test data success."
else 
    echo "Error! ${TEST_PATH} is not exists."
fi

cd ${OUTPUT_PATH}
wc -l train.txt test.txt

# Fetch RankLib-1.0.0-mdf-2.11-SNAPSHOT.jar
${HADOOP} ${ZJY_CLUSTER} fs -get /user/h_ms/xam/lambdamart/RankLib-1.0.0-mdf-2.11-SNAPSHOT.jar ${OUTPUT_PATH}/RankLib-1.0.0-mdf-2.11-SNAPSHOT.jar
echo "Fetch RankLib success."



