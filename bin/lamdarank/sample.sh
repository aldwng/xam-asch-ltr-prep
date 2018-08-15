#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/..; pwd -P`
echo "bin dir: ${BIN_DIR}"

source ${BIN_DIR}/util/util.sh

run label.DataRawGenerator --day ${day}
run label.RankInstanceGenerator --day ${day}
run sample.SampleGenerator --day ${day}
run sample.FeaMapGenerator --day ${day}
run sample.RankSampleGenerator --day ${day}

