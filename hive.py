from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext


SparkContext.setSystemProperty("hive.metastore.uris", "thrift://nn1:9083")
sparkSession = (SparkSession.builder.appName('hive_connection').enableHiveSupport().getOrCreate())
sparkSession.sparkContext.setLogLevel("ERROR")

#%% actualziar las tablas STAGE con respecto al ciclo
ciclo = '201808'
hive_command = sparkSession.sql('USE MEDIX')
hive_command = sparkSession.sql('INSERT OVERWRITE TABLE stage_visitas SELECT * FROM EXT_TABLE_visitas WHERE vis_ciclo='+ciclo)
hive_command = sparkSession.sql('INSERT OVERWRITE TABLE stage_cobertura SELECT * FROM EXT_TABLE_cobertura WHERE cob_ciclo='+ciclo)
hive_command = sparkSession.sql('INSERT OVERWRITE TABLE stage_cliente_crm  SELECT * FROM EXT_TABLE_clientes_crm WHERE cli_ciclo='+ciclo)

#%% crear stage_reporte_anual

#import pandas as pd
#path_read = r'C:\Users\DELL\Dropbox\Christopher_Medix\CRM\Fuerza_ventas\Visitas\cierre_ciclo\Ciclo 8\CRM_Visitas_rev.csv'
#visitas = pd.read_csv(path_read,low_memory=False)

clientes = sparkSession.sql('SELECT * FROM STAGE_CLIENTE_CRM')
visitas = sparkSession.sql('SELECT * FROM STAGE_VISITAS')
cobertura = sparkSession.sql('SELECT * FROM STAGE_COBERTURA')



hive_command.show()


