#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/..; pwd -P`
echo "bin dir: ${BIN_DIR}"

source ${BIN_DIR}/util/util.sh

run prepare.AppGenerator --day ${day}
run prepare.LabelGenerator --start ${week} --end ${day}
run prepare.SampleGenerator --day ${day}

