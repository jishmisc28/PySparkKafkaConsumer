#!/usr/bin/python
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col,from_unixtime,from_utc_timestamp,to_date,element_at,split
import argparse
from datetime import datetime
from datetime import timedelta
#import pytz
import time
import json
import kafka

parser = argparse.ArgumentParser()
parser.add_argument("--app_name", help=" applicaton name" ,default=None)
parser.add_argument("--topic", help="kafka topic name")
parser.add_argument("--broker", help="kafka broker ips")
parser.add_argument("--partitionColOn", help="column name on which to create partiton")
parser.add_argument("--partitionedColName", help="column name of created partiton")
parser.add_argument("--noKafkaPartitions", help="number of kafka partitons")
parser.add_argument("--outPath", help="output path for macro batch ingestion")
parser.add_argument("--startDate", help="start date for offset fetch")
parser.add_argument("--endDate", help="end date for offset fetch")
parser.add_argument("--batchHour", help="hours to batch ingestion",default=4)


# list of arguments to parse - total 10
args = parser.parse_args()
app_name = args.app_name
topic = args.topic
broker = args.broker
partitionColOn = args.partitionColOn
partitionedColName = args.partitionedColName
noKafkaPartitions = args.noKafkaPartitions
outPath = args.outPath
startDate = args.startDate
endDate = args.endDate
batchHour = args.batchHour

try:
    module = __import__('schema')
    schema = getattr(module, app_name)
except AttributeError as e:
    print(e)
    raise e

kafka_server = broker
s3bucket = "datalake-jg"

#now = datetime.now(pytz.timezone('Asia/Kolkata'))
#current_date = startDate.strftime("%Y-%m-%d")

spark = SparkSession \
    .builder \
    .getOrCreate()

#.config("spark.pyspark.virtualenv.enabled","true")\

#spark.sparkContext.install_pypi_package("kafka")
spark.sparkContext.setLogLevel("ERROR")
#spark.conf.set("spark.pyspark.virtualenv.enabled", "true")

def json_parser(df,schema):
    string_df = df.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
    parsed_df = string_df.select(["parsed_value."+field.name for field in schema])
    parsed_df = parsed_df.withColumn(partitionedColName,to_date(col(partitionColOn),'yyyy-MM-dd'))
    #parsed_df = parsed_df.withColumn(partitionedColName,to_date(col(partitionColOn),'yyyy-MM-dd')).filter("dt == '{}'".format(current_date))
    return parsed_df

def to_seconds(date):
    return time.mktime(date.timetuple())

def get_kafka_offsets(start_time, end_time):

    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(topic, bootstrap_servers=broker, enable_auto_commit=True)
    consumer.poll()

    no_of_partition = noKafkaPartitions
    i = 0
    star_offset = {topic: {}}
    end_offset = {topic: {}}
    while i < no_of_partition:
        tp = TopicPartition(topic, i)  # partition n. 0
        # in simple case without any special kafka configuration there is only one partition for each topic channel
        # and it's number is 0

        # in fact you asked about how to use 2 methods: offsets_for_times() and seek()
        rec_in = consumer.offsets_for_times({tp: to_seconds(start_time) * 1000})
        rec_out = consumer.offsets_for_times({tp: to_seconds(end_time) * 1000})

        #print(to_seconds(start_time),to_seconds(end_time))
        star_offset[topic][i] = rec_in[tp].offset
        end_offset[topic][i] = rec_out[tp].offset

        i += 1

    return json.dumps(star_offset), json.dumps(end_offset)


if __name__ == "__main__":

    start_date = datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(endDate, '%Y-%m-%d %H:%M:%S')

    while (start_date!=end_date):
        next_date = start_date + timedelta(hours=batchHour)
        print("Initiating macro batch read for {} ..".format(next_date))

        # debug steps
        #print("******************")
        #print(start_date,next_date)
        #starting_offset, ending_offset = get_offsets(datetime(start_date), datetime(next_date))
        #print(starting_offset, ending_offset)

        # receive offset ranges based on input dates
        starting_offset, ending_offset = get_kafka_offsets(start_date, next_date)

        # start reading from kafka
        macro_batch_df = spark.read.format("kafka") \
            .option("kafka.bootstrap.servers",kafka_server) \
            .option("subscribe",topic) \
            .option("startingOffsets", starting_offset) \
            .option("endingOffsets", ending_offset) \
            .load()

        # parse the payload using json schema
        parsed_df = json_parser(macro_batch_df,schema)

        # save macro batch data in parquet
        parsed_df.write \
            .partitionBy(partitionedColName) \
            .mode("append") \
            .parquet(outPath)
        print("Finished macro batch write for {} ..".format(next_date))

        # increment
        start_date = next_date

    print("******************")
    print("Kafka Batchfill Job Succeeded In Writing Data At {} For Date Range {} - {}".format(outPath,startDate,endDate))
    print("******************")

