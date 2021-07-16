#!/bin/bash

lines=`cat /home/hadoop/APPNAME/logs/output.log | wc -l`
if [ $lines -gt 10000 ]
then
        sed -i '1,5d' /home/hadoop/APPNAME/logs/output.log
fi

echo "Running Spark job"
app_name=$1
topic=$2
echo "******************$app_name, $topic ***********************************"
count=`yarn application -list -appStates RUNNING | grep $app_name | wc -l`
count2=`yarn application -list -appStates ACCEPTED | grep $app_name | wc -l`
echo $count

if [ $count -ge 1 ] || [ $count2 -ge 1 ]
then
        echo "$app_name spark job is running."
else
        echo "$app_name spark job is failed or killed or completed. Attempt to re run the job......"
        source ~/.bashrc
        nohup spark-submit --master yarn \
            --deploy-mode cluster \
            --name $app_name \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:2.4.4 \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.blacklist.killBlacklistedExecutors=true \
            --conf spark.dynamicAllocation.maxExecutors=10 \
            --conf spark.dynamicAllocation.minExecutors=4 \
            --conf spark.driver.maxResultSize=4g \
            --conf spark.executor.memoryOverhead=2g \
	          --conf spark.sql.streaming.minBatchesToRetain=1 \
	          --conf spark.sql.streaming.fileSink.log.deletion=true \
	          --conf spark.sql.streaming.fileSink.log.compactInterval=3 \
	          --conf spark.sql.streaming.fileSink.log.cleanupDelay=0 \
            --driver-memory=4g \
            --executor-memory=4g \
            --executor-cores=4 \
            --driver-cores=2 \
            --py-files /home/hadoop/APPNAME/schema.py \
            /home/hadoop/APPNAME/APPNAME.py --app_name $app_name --topic $topic >> /home/hadoop/APPNAME/logs/output.log 2>&1 &

fi
