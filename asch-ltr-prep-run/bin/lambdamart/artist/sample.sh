#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/../..; pwd -P`
echo "bin dir: ${BIN_DIR}"

source ${BIN_DIR}/util/util.sh

run net.xam.ltr.prep.run.prepare.artist.SampleGenerator --day ${day}

