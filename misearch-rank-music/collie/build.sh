#! /bin/bash

COLLIE_DIR=`cd $(dirname $0); pwd -P`
MUSIC_RANK_DIR=`cd $(dirname ${COLLIE_DIR}); pwd -P`
PROJECT_DIR=`cd $(dirname ${MUSIC_RANK_DIR}); pwd -P`
echo "collie dir: ${COLLIE_DIR}"
echo "music rank dir: ${MUSIC_RANK_DIR}"
echo "project dir: ${PROJECT_DIR}"

BIN_DIR=${MUSIC_RANK_DIR}/bin
TARGET_DIR=${MUSIC_RANK_DIR}/target
echo "bin dir: ${BIN_DIR}"
echo "target dir: ${TARGET_DIR}"

cd ${PROJECT_DIR}
mvn clean package -pl misearch-rank-music -am

rm ${COLLIE_DIR}/misearch-rank-music.zip
cp ${TARGET_DIR}/misearch-rank-music.jar ${MUSIC_RANK_DIR}/misearch-rank-music.jar

cd ${MUSIC_RANK_DIR}
pwd
zip -r ${COLLIE_DIR}/misearch-rank-music.zip bin/* misearch-rank-music.jar
rm ${MUSIC_RANK_DIR}/misearch-rank-music.jar