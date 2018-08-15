#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/../..; pwd -P`
echo "bin dir: ${BIN_DIR}"

DATA_PATH="${BIN_DIR}/data"

test="${DATA_PATH}/test.txt"
model="${DATA_PATH}/model.txt"
jar="${DATA_PATH}/ranklib-2.10-snapshot.jar"

JAVA="/usr/java/jdk1.8.0_144/bin/java"
JAVA_OPTS="-Xms512M -Xmx10g"

function run {
    ${JAVA} ${JAVA_OPTS} -jar ${jar} \
    -load ${model} \
    -test ${test} \
    -sparse \
    -norm zscore \
    -ranker 6 \
    -metric2T "$1"
}

if [ -e "${model}" ] && [ -e "${test}" ] ; then
    run NDCG@1
    run NDCG@5
    run NDCG@10
    run NDCG@20
fi

