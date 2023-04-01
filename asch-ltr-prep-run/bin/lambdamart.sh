#!/usr/bin/env bash

BIN_DIR=`cd $(dirname $0); pwd -P`
echo "bin dir: ${BIN_DIR}"

source ${BIN_DIR}/util/util.sh

bash ${BIN_DIR}/lambdamart/sample.sh ${day}
#bash ${BIN_DIR}/lambdamart/train.sh ${day}