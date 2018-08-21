#!/usr/bin/env bash

BIN_DIR=`cd $(dirname $0); pwd -P`
echo "bin dir: ${BIN_DIR}"

source ${BIN_DIR}/util/util.sh

LOGS="${BIN_DIR}/logs"

mkdir -p ${LOGS}
bash ${BIN_DIR}/base/base.sh ${day} >> ${LOGS}/base.log 2>&1
bash ${BIN_DIR}/lamdarank/sample.sh ${day} >> ${LOGS}/sample.log 2>&1
bash ${BIN_DIR}/lamdarank/train.sh ${day} >> ${LOGS}/model.log 2>&1
bash ${BIN_DIR}/lamdarank/predict.sh ${day} >> ${LOGS}/predict.log 2>&1