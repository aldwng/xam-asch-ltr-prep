#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/../..; pwd -P`
echo "bin dir: ${BIN_DIR}"

DATA_PATH="${BIN_DIR}/data"

if [ $# != 1 ]; then
  day=`date --date="1 days ago" "+%Y%m%d"`
else
  day=$1
fi

echo "train date: ${day}"

HADOOP="${HADOOP_HOME}/bin/hadoop"
C3_CLUSTER="--cluster c3prc-hadoop"

ls -lh ${DATA_PATH}/*

train="${DATA_PATH}/train.txt"
test="${DATA_PATH}/test.txt"
validate="${DATA_PATH}/validate.txt"

jar="${DATA_PATH}/ranklib-2.10-snapshot.jar"

model="${DATA_PATH}/model.txt"
rm ${model}

JAVA="/usr/java/jdk1.8.0_144/bin/java"
JAVA_OPTS="-Xms512M -Xmx10g"

function run {
${JAVA} ${JAVA_OPTS} -jar ${jar} \
    -train ${train} \
    -test ${test} \
    -validate ${validate} \
    -sparse \
    -norm zscore \
    -ranker 6 \
    -metric2t NDCG@30 \
    -metric2T NDCG@30 \
    -gmax 4 \
    -tree 200 \
    -leaf 20 \
    -shrinkage 0.1 \
    -tc -1 \
    -mls 1 \
    -estop 10 \
    -save ${model}
}

if [ -e "${train}" ] && [ -e "${test}" ] && [ -e "${validate}" ] && [ -e "${jar}" ];
then
  run
  ${HADOOP} ${C3_CLUSTER} fs -put -f ${model} /user/h_misearch/appmarket/rank/model/${model}
  ${HADOOP} ${C3_CLUSTER} fs -put -f ${model} /user/h_misearch/appmarket/rank/model/${model}.${day}
  echo "Upload model.txt success!"
else
    echo "Error! model train data files are not exist!"
fi


