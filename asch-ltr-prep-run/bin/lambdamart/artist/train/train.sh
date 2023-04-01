#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/../../..; pwd -P`
echo "bin dir: ${BIN_DIR}"

DATA_PATH="${BIN_DIR}/data_artist"

if [ $# != 1 ]; then
  day=`date --date="1 days ago" "+%Y%m%d"`
else
  day=$1
fi

echo "train date: ${day}"

export HADOOP_OPTS="-Dhadoop.property.hadoop.client.keytab.file=/etc/h_ms.keytab \
                    -Dhadoop.property.hadoop.client.kerberos.principal=h_ms@XMH"

HADOOP_HOME="/home/work/tars/infra-client"
HADOOP="${HADOOP_HOME}/bin/hadoop"
ZJY_CLUSTER="--cluster zjyprc-hadoop"

ls -lh ${DATA_PATH}/*

train="${DATA_PATH}/train.txt"
test="${DATA_PATH}/test.txt"

jar="${DATA_PATH}/RankLib-1.0.0-mdf-2.11-SNAPSHOT.jar"
model="${DATA_PATH}/model.txt"
rm ${model}

JAVA="/usr/java/openjdk1.8.0_202/bin/java"
if [ -n "${JAVA8_HOME}" ]; then
    JAVA=${JAVA8_HOME}/bin/java
elif [ -n "${JAVA_HOME}" ]; then
    JAVA=${JAVA_HOME}/bin/java
fi
echo ${JAVA}

JAVA_OPTS="-Xms1g -Xmx15g"

function run {
${JAVA} ${JAVA_OPTS} -jar ${jar} \
    -train ${train} \
    -test ${test} \
    -norm zscore \
    -ranker 6 \
    -metric2t NDCG@3 \
    -metric2T NDCG@3 \
    -gmax 5 \
    -tree 1000 \
    -leaf 10 \
    -shrinkage 0.1 \
    -tc -1 \
    -mls 1 \
    -estop 200 \
    -tvs 0.8 \
    -save ${model}
}

if [ -e "${train}" ] && [ -e "${test}" ] && [ -e "${jar}" ];
then
  run
  if [ $? -eq 0 ]; then
      echo "train model suc: ${day}"
  else
      echo "train model fail: ${day}"
  fi

  ${HADOOP} ${ZJY_CLUSTER} fs -put -f ${model} /user/h_ms/xam/lambdamart_artist/model/model.txt
  ${HADOOP} ${ZJY_CLUSTER} fs -put -f ${model} /user/h_ms/xam/lambdamart_artist/model/model.txt.${day}
  echo "Upload model.txt success!"
else
  echo "Error! model train data files are not exist!"
fi


