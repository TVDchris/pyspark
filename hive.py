from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

SparkContext.setSystemProperty("hive.metastore.uris", "thrift://nn1:9083")
sparkSession = (SparkSession.builder.appName('hive_connection').enableHiveSupport().getOrCreate())
sparkSession.sparkContext.setLogLevel("ERROR")

#%% actualziar las tablas STAGE con respecto al ciclo
#ciclo = '201808'
hive_command = sparkSession.sql('USE MEDIX')
hive_command = sparkSession.sql('INSERT OVERWRITE TABLE stage_visitas SELECT * FROM EXT_TABLE_visitas')# WHERE vis_ciclo='+ciclo)
hive_command = sparkSession.sql('INSERT OVERWRITE TABLE stage_cobertura SELECT * FROM EXT_TABLE_cobertura')# WHERE cob_ciclo='+ciclo)
hive_command = sparkSession.sql('INSERT OVERWRITE TABLE stage_cliente_crm  SELECT * FROM EXT_TABLE_clientes_crm')# WHERE cli_ciclo='+ciclo)

#%% crear stage_reporte_anual

cliente = sparkSession.sql('SELECT * FROM STAGE_CLIENTE_CRM')
visitas = sparkSession.sql('SELECT * FROM STAGE_VISITAS')
cobertura = sparkSession.sql('SELECT * FROM STAGE_COBERTURA')

Ta = cliente.alias ('Ta')
Tb = visitas.alias ('Tb')
Tc = cobertura.alias ('Tc')

from pyspark.sql import functions as F
Ta = Ta.withColumn('cli_key', F.upper(F.col('cli_key')))
Ta = Ta.withColumn('cli_clienteid', F.upper(F.col('cli_clienteid')))
Tb = Tb.withColumn('vis_id_cliente', F.upper(F.col('vis_id_cliente')))


reporte_anual = Tb.withColumn('vis_key',F.upper(F.concat(F.col('vis_id_cliente'), F.lit(' '), F.col('vis_ruta'))))\
                  .where( (F.col('vis_estatus')=='Registrada') &\
                  (F.col('vis_estatus_cliente') =='Activo'))
Ra = reporte_anual.alias ('Ra')
reporte_anual= Ra.join (Ta,[ F.upper(Ta.cli_key)== F.upper(Ra.vis_key), Ta.cli_ciclo == Ra.vis_ciclo], how='left')\
                  .select(F.col('vis_ciclo'),F.col('cli_distrito'),F.col('cli_linea'),F.col('cli_ruta'),F.col('cli_repre')\
                  ,F.col('cli_clienteid'),F.col('cli_estatus'),F.col('cli_tipo'),F.col('cli_frecuencia'))\
                  .groupBy('vis_ciclo','cli_distrito','cli_linea','cli_ruta','cli_repre','cli_clienteid','cli_estatus',\
                  'cli_tipo','cli_frecuencia').count()


reporte_anual = reporte_anual.groupBy('cli_distrito','cli_linea','cli_ruta','cli_repre','cli_clienteid','cli_estatus','cli_tipo','cli_frecuencia').pivot('vis_ciclo').sum('count')

reporte_anual.select('cli_clienteid').where(F.col('vis_ciclo')=='201806').show()
'''
reporte_anual.select('cli_frecuencia','201806','201808').where((F.col('cli_ruta') == 'SYO-1206') & (F.col('cli_tipo') == 'MÉDICO') &\
                                                              (F.col('cli_clienteid') == 'AAAD94F3-395C-4AA2-8888-DE6B857EB204')).show()


reporte_anual.select(Ra).where((F.col('cli_ruta') == 'SYO-1206') & (F.col('cli_tipo') == 'MÉDICO') &\
                              (F.col('cli_clienteid') == 'AAAD94F3-395C-4AA2-8888-DE6B857EB204')).show()


reporte_anual.select(F.upper(F.col('cli_clienteid'))).where(F.col('vis_ciclo')=='201806').show()
'''