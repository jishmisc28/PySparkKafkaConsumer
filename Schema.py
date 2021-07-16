from pyspark.sql.types import *

Topic_Schema = StructType([ 
    StructField("TableId", LongType(), True),
    StructField("TableSS", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("LogTime", StringType(), True),
    StructField("MachineAddress", StringType(), True),
    StructField("CardArrangement", StringType(), True),
    StructField("EventName", StringType(), True),
    StructField("HandId", LongType(), True),
    StructField("TableConfigId", LongType(), True),
    StructField("DeviceType", StringType(), True),
	  StructField("UserId", LongType(), True),
    StructField("UserSS", FloatType(), True),
    StructField("RoundNumber", IntegerType(), True),
    StructField("TournamentId", IntegerType(), True),
    StructField("IpAddress", StringType(), True),
    StructField("GameId", LongType(), True),
    StructField("GameType", StringType(), True),
    StructField("GameVariant", StringType(), True),
    StructField("PoolPoints", IntegerType(), True),
    StructField("DeviceName", StringType(), True),
    StructField("ApkVersion", StringType(), True),
    StructField("Imei", StringType(), True),
    StructField("OsName", StringType(), True),
    StructField("OsVersion", StringType(), True),
    StructField("BrowserName", StringType(), True),
    StructField("BrowserVersion", StringType(), True),
    StructField("Env", StringType(), True)

])

