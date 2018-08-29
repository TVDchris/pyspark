from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext


spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

SparkContext.setSystemProperty("hive.metastore.uris", "thrift://nn1:9083").setLogLevel("ERROR")
sparkSession = (SparkSession.builder.appName('hive_connection').enableHiveSupport().getOrCreate())
hive_command = sparkSession.sql('USE MEDIX')
hive_command = sparkSession.sql('SELECT COUNT(*) FROM EXT_TABLE_VISITAS')

