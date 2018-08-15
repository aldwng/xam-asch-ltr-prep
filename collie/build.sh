#! /bin/bash

COLLIE_DIR=`cd $(dirname $0); pwd -P`
PROJECT_DIR=`cd $(dirname ${COLLIE_DIR}); pwd -P`
echo "collie dir: ${COLLIE_DIR}"
echo "project dir: ${PROJECT_DIR}"

BIN_DIR=${PROJECT_DIR}/bin
TARGET_DIR=${PROJECT_DIR}/target
echo "bin dir: ${BIN_DIR}"
echo "target dir: ${TARGET_DIR}"

cd ${PROJECT_DIR}
mvn clean package

rm ${COLLIE_DIR}/appsearch-rank.zip
cp ${TARGET_DIR}/appsearch-rank.jar ${PROJECT_DIR}/appsearch-rank.jar

pwd
zip -r ${COLLIE_DIR}/appsearch-rank.zip bin/* appsearch-rank.jar
rm ${PROJECT_DIR}/appsearch-rank.jar