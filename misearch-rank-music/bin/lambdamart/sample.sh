#!/usr/bin/env bash

CURRENT_DIR=`cd $(dirname $0); pwd -P`
BIN_DIR=`cd ${CURRENT_DIR}/..; pwd -P`
echo "bin dir: ${BIN_DIR}"

source ${BIN_DIR}/util/util.sh

run com.xiaomi.misearch.rank.music.prepare.LabelGenerator --start ${week} --end ${day}
run com.xiaomi.misearch.rank.music.prepare.TagOneHotIndexGenerator
run com.xiaomi.misearch.rank.music.prepare.FeatureGenerator
run com.xiaomi.misearch.rank.music.prepare.SampleGenerator --day ${day}

