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

HADOOP="${HADOOP_HOME}/bin/hadoop"
C3_CLUSTER="--cluster c3prc-hadoop"

SAMPLE_PATH_PREFIX="/user/h_misearch/appmarket/rank/train/rank_sample/date=${day}"
TRAIN_PATH=${SAMPLE_PATH_PREFIX}/train
TEST_PATH=${SAMPLE_PATH_PREFIX}/test
VALIDATE_PATH=${SAMPLE_PATH_PREFIX}/validate

OUTPUT_PATH="${BIN_DIR}/data"
rm -rf ${OUTPUT_PATH}
mkdir ${OUTPUT_PATH}

# Fetch train data
${HADOOP} ${C3_CLUSTER} fs -test -e ${TRAIN_PATH}/_SUCCESS
if [ $? -eq 0 ] ;then
    ${HADOOP} ${C3_CLUSTER} fs -cat ${TRAIN_PATH}/* > ${OUTPUT_PATH}/train.txt
    echo "Fetch train data success."
else 
    echo "Error! ${TRAIN_PATH} is not exists."
fi 

# Fetch test data
${HADOOP} ${C3_CLUSTER} fs -test -e ${TEST_PATH}/_SUCCESS
if [ $? -eq 0 ] ;then
    ${HADOOP} ${C3_CLUSTER} fs -cat ${TEST_PATH}/* > ${OUTPUT_PATH}/test.txt
    echo "Fetch test data success."
else 
    echo "Error! ${TEST_PATH} is not exists."
fi

# Fetch validate data
${HADOOP} ${C3_CLUSTER} fs -test -e ${VALIDATE_PATH}/_SUCCESS
if [ $? -eq 0 ] ;then
    ${HADOOP} ${C3_CLUSTER} fs -cat ${VALIDATE_PATH}/* > ${OUTPUT_PATH}/validate.txt
    echo "Fetch validate data success."
else
    echo "Error! ${VALIDATE_PATH} is not exists."
fi

cd ${OUTPUT_PATH}
wc -l train.txt test.text validate.txt

# Fetch ranklib-2.10-snapshot.jar
${HADOOP} ${C3_CLUSTER} fs -get /user/h_misearch/appmarket/rank/ranklib-2.10-snapshot.jar ${OUTPUT_PATH}/ranklib-2.10-snapshot.jar
echo "Fetch RankLib success."



