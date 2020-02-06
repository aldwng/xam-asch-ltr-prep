#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/../..; pwd -P`
echo "bin dir: ${BIN_DIR}"

source ${BIN_DIR}/util/util.sh

bash ${BIN_DIR}/lambdamart/artist/train/fetch.sh ${day}
bash ${BIN_DIR}/lambdamart/artist/train/train.sh ${day}
#bash ${BIN_DIR}/train/test.sh

