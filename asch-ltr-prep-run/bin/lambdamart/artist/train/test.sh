#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/../../..; pwd -P`
echo "bin dir: ${BIN_DIR}"

DATA_PATH="${BIN_DIR}/data/artist"

test="${DATA_PATH}/test.txt"
model="${DATA_PATH}/model.txt"
jar="${DATA_PATH}/RankLib-1.0.0-mdf-2.11-SNAPSHOT.jar"

JAVA="/usr/java/openjdk1.8.0_202/bin/java"
JAVA_OPTS="-Xms1g -Xmx15g"

function run {
    ${JAVA} ${JAVA_OPTS} -jar ${jar} \
    -load ${model} \
    -test ${test} \
    -norm zscore \
    -gmax 5 \
    -ranker 6 \
    -metric2T "$1" \
    -idv "$2"
}

if [ -e "${model}" ] && [ -e "${test}" ] ; then
    run NDCG@3 ${DATA_PATH}/test.ndcg3.txt
    run NDCG@5 ${DATA_PATH}/test.ndcg5.txt
    run NDCG@10 ${DATA_PATH}/test.ndcg10.txt
fi

