from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://nn1:9083")
sparkSession = (SparkSession.builder.appName('hive_connection').enableHiveSupport().getOrCreate())
visitas = sparkSession.sql('USE MEDIX')
visitas = sparkSession.sql('SELECT * FROM STAGE_VISITAS WHERE VIS_CICLO = 201808')
cliente = sparkSession.sql('SELECT * FROM STAGE_CLIENTE_CRM WHERE CLI_CICLO = 201808')

