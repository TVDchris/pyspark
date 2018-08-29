from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext


sparkContext.setSystemProperty("hive.metastore.uris", "thrift://nn1:9083")
sparkSession = (SparkSession.builder.appName('hive_connection').enableHiveSupport().getOrCreate())
hive_command = sparkSession.sql('USE MEDIX')
hive_command = sparkSession.sql('SELECT COUNT(*) FROM EXT_TABLE_VISITAS')
hive_command.show()
