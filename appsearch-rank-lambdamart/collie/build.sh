#! /bin/bash

COLLIE_DIR=`cd $(dirname $0); pwd -P`
LAMBDAMART_DIR=`cd $(dirname ${COLLIE_DIR}); pwd -P`
PROJECT_DIR=`cd $(dirname ${LAMBDAMART_DIR}); pwd -P`
echo "collie dir: ${COLLIE_DIR}"
echo "lambdamart dir: ${LAMBDAMART_DIR}"
echo "project dir: ${PROJECT_DIR}"

BIN_DIR=${LAMBDAMART_DIR}/bin
TARGET_DIR=${LAMBDAMART_DIR}/target
echo "bin dir: ${BIN_DIR}"
echo "target dir: ${TARGET_DIR}"

cd ${PROJECT_DIR}
mvn clean package -pl appsearch-rank-lambdamart -am

rm ${COLLIE_DIR}/appsearch-rank.zip
cp ${TARGET_DIR}/appsearch-rank.jar ${LAMBDAMART_DIR}/appsearch-rank.jar

cd ${LAMBDAMART_DIR}
pwd
zip -r ${COLLIE_DIR}/appsearch-rank.zip bin/* appsearch-rank.jar
rm ${LAMBDAMART_DIR}/appsearch-rank.jar