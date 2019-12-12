#!/usr/bin/env bash

if [ $# != 1 ]; then
  day=`date --date="1 days ago" "+%Y%m%d"`
else
  day=$1
fi

week=`date -d "$day -7 day" +%Y%m%d`

echo "train: $week $day"

export SPARK_SUBMIT_OPTS="-Dhadoop.property.hadoop.client.keytab.file=/etc/h_misearch.keytab \
                          -Dhadoop.property.hadoop.client.kerberos.principal=h_misearch@XIAOMI.HADOOP"
function run {
     /home/work/tars/infra-client/bin/spark-submit \
        --java 8 \
        --cluster zjyprc-hadoop-spark2.1 \
        --class "$1" \
        --master yarn-cluster \
        --queue service.cloud_group.sns.search_rec \
        --num-executors 100 \
        --driver-memory 10g \
        --executor-memory 10g \
        --conf spark.yarn.job.owners=wangshenglan \
        --conf spark.yarn.alert.phone.number=13581916953 \
        --conf spark.hadoop.validateOutputSpecs=false \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.yarn.executor.memoryOverhead=3g \
        --conf spark.driver.maxResultSize=4g \
        --conf spark.kryoserializer.buffer.max=1g \
        --conf spark.yarn.appMasterEnv.JAVA_HOME=/opt/soft/jdk1.8.0 \
        --conf spark.executorEnv.JAVA_HOME=/opt/soft/jdk1.8.0 \
        --conf spark.speculation=true \
        --conf spark.executor.extraJavaOptions="-XX:MaxDirectMemorySize=8192m" \
        misearch-rank-music.jar "${@:2}"
}
