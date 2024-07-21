from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	

my_schema = StructType() \
    .add("request_id",StringType()) \
    .add("campaign_id",StringType()) \
    .add("user_id",StringType()) \
    .add("click",IntegerType()) \
    .add("view",IntegerType()) \
    .add("acquisition",IntegerType()) \
    .add("auction_cpm",DoubleType()) \
    .add("auction_cpc",DoubleType()) \
    .add("auction_cpa",DoubleType()) \
    .add("target_age_range",StringType()) \
    .add("target_Location",StringType()) \
    .add("target_gender",StringType()) \
    .add("target_income_bucket",StringType()) \
    .add("target_device_type",StringType()) \
    .add("campaign_start_time",StringType()) \
    .add("campaign_end_time",StringType()) \
    .add("user_action",StringType()) \
    .add("expenditure",DoubleType()) \
    .add("timestamp",StringType())
	
lines = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","44.214.172.144:9092")  \
	.option("subscribe","user-feedback")  \
	.option("startingOffsets","earliest")  \
	.load()

kafkaDF = lines.select(from_json(col("value").cast("string"),my_schema).alias("data")).select("data.*")	

hdfs_sink = kafkaDF \
    .writeStream \
    .format('csv') \
    .outputMode("append") \
    .option("truncate","false") \
    .option("path","/user/hadoop/op1") \
    .option("checkpointLocation","/user/hadoop/cp1") \
    .trigger(processingTime="1 minute") \
    .start()

	
hdfs_sink.awaitTermination()
