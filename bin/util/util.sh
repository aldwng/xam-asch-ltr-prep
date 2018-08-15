#!/usr/bin/env bash

if [ $# != 1 ]; then
  day=`date --date="1 days ago" "+%Y%m%d"`
else
  day=$1
fi

month=`date -d "$day -30 day" +%Y%m%d`
week=`date -d "$day -7 day" +%Y%m%d`

echo "train: $month $week $day"

function run {
     /home/work/tars/infra-client/bin/spark-submit \
        --java 8 \
        --cluster c3prc-hadoop \
        --class "$1" \
        --master yarn-cluster \
        --queue service.cloud_group.sns.queue_1 \
        --num-executors 100 \
        --driver-memory 8g \
        --executor-memory 8g \
        --conf spark.yarn.job.owners=wangshenglan \
        --conf spark.yarn.alert.phone.number=13581916953 \
        --conf spark.hadoop.validateOutputSpecs=false \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.yarn.executor.memoryOverhead=3g \
        --conf spark.kryoserializer.buffer.max=1g \
        --conf spark.yarn.appMasterEnv.JAVA_HOME=/opt/soft/jdk1.8.0 \
        --conf spark.executorEnv.JAVA_HOME=/opt/soft/jdk1.8.0 \
        --conf spark.speculation=true \
        --conf spark.executor.extraJavaOptions="-XX:MaxDirectMemorySize=1024m" \
        appsearch-rank.jar "${@:2}"
}