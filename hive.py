# -*- coding: utf-8 -*-
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
reporte_anual = reporte_anual.groupBy('cli_distrito','cli_linea','cli_ruta','cli_repre','cli_clienteid','cli_estatus',\
                                      'cli_tipo','cli_frecuencia')\
                             .pivot('vis_ciclo').sum('count')
Ra = reporte_anual.alias ('Ra')
Ra = Ra.withColumn('cli_ciclo',F.lit(ciclo))
#%% CIERRE DE CICLO
#%%   #################################################### MEDICOS#####################################################################################
cierre_ciclo = Ra.select('cli_ciclo','cli_tipo','cli_linea','cli_distrito','cli_ruta','cli_repre')\
                 .where ( F.col('cli_tipo') == 'MÉDICO')\
                 .dropDuplicates()
cc = cierre_ciclo.alias('cc')
#%% territorio
repres = sparkSession.sql('SELECT * FROM STAGE_REPRESENTANTES')
a = cc.select('cli_ruta').join(repres, cc.cli_ruta== repres.rep_ruta,how='left').select(F.col('cli_ruta')\
                         .alias('rep_ruta'),'rep_territorio')\
                         .dropDuplicates()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.rep_ruta ,how='left').select('cc.*',F.col('rep_territorio').alias('territorio'))
cc = cierre_ciclo.alias('cc')
#%% plan_trabajo
a = Ta.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_frecuencia') != 0) &\
                                                 (F.col('cli_tipo') == ('MÉDICO')))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('plan_trabajo'))
cc = cierre_ciclo.alias('cc')
#%% medicos_visitas
a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('medicos_visitados'))
cc = cierre_ciclo.alias('cc')
#%% frecuencia 0
a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')) & \
                                                 (F.col('cli_frecuencia') == 0) & \
                                                 (F.col(ciclo) == 1))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F0_1'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')) & \
                                                 (F.col('cli_frecuencia') == 0) & \
                                                 (F.col(ciclo) == 2))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F0_2'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')) & \
                                                 (F.col('cli_frecuencia') == 0) & \
                                                 (F.col(ciclo) >= 3))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F0_3'))
cc = cierre_ciclo.alias('cc')
#%% frecuencia 1
a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')) & \
                                                 (F.col('cli_frecuencia') == 1) & \
                                                 (F.col(ciclo) == 1))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F1_1'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')) & \
                                                 (F.col('cli_frecuencia') == 1) & \
                                                 (F.col(ciclo) == 2))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F1_2'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')) & \
                                                 (F.col('cli_frecuencia') == 1) & \
                                                 (F.col(ciclo) >= 3))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F1_3'))
cc = cierre_ciclo.alias('cc')

#%% frecuencia 2
a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')) & \
                                                 (F.col('cli_frecuencia') == 2) & \
                                                 (F.col(ciclo) == 1))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F2_1'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')) & \
                                                 (F.col('cli_frecuencia') == 2) & \
                                                 (F.col(ciclo) == 2))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F2_2'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('MÉDICO')) & \
                                                 (F.col('cli_frecuencia') == 2) & \
                                                 (F.col(ciclo) >= 3))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F2_3'))
cc = cierre_ciclo.alias('cc')
#%% COBERTURA
a = Tc.select('cob_ruta','cob_vis_registradas','cob_vis_objetivo','cob_vis_descontadas')\
      .filter((F.col('cob_tipo_cliente') == ('médico')))
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cob_ruta ,how='left')\
                 .select('cc.*',F.col('cob_vis_registradas').alias('visitas_reg'),\
                               F.col('cob_vis_objetivo').alias('visitas_obj'),\
                               F.col('cob_vis_descontadas').alias('visitas_desc'))
cc = cierre_ciclo.alias('cc')
#%%visitasacompañadas
a = Tb.select('vis_ruta','vis_acomp').filter((F.col('vis_acomp').isin(['GERENTE REGIONAL','Acompañado por Gerente'])) & \
                                                (F.col('vis_tipo_cliente') == ('médico')))\
                                     .groupby('vis_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.vis_ruta ,how='left')\
                 .select('cc.*',F.col('count').alias('visitas_acomp'))
cierre_ciclo = cierre_ciclo.fillna(0,subset=['F0_1','F0_2','F0_3','F1_1','F1_2','F1_3','F2_1','F2_2','F2_3',\
                                             'visitas_reg','visitas_obj','visitas_desc','visitas_acomp'])

cierre_ciclo = cierre_ciclo.withColumn('plan_trabajo',cierre_ciclo['plan_trabajo'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('medicos_visitados',cierre_ciclo['medicos_visitados'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F0_1',cierre_ciclo['F0_1'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F0_2',cierre_ciclo['F0_2'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F0_3',cierre_ciclo['F0_3'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F1_1',cierre_ciclo['F1_1'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F1_2',cierre_ciclo['F1_2'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F1_3',cierre_ciclo['F1_3'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F2_1',cierre_ciclo['F2_1'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F2_2',cierre_ciclo['F2_2'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F2_3',cierre_ciclo['F2_3'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_reg',cierre_ciclo['visitas_reg'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_obj',cierre_ciclo['visitas_obj'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_desc',cierre_ciclo['visitas_desc'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_acomp',cierre_ciclo['visitas_acomp'].cast('Int'))

cierre_ciclo = cierre_ciclo.withColumn('cli_ruta_nombre',(F.concat(F.col('cli_ruta'), F.lit('/'), F.col('cli_repre'))))
cc = cierre_ciclo.alias('cc')

hive_command = sparkSession.sql('DROP TABLE IF EXISTS STAGE_CIERRE_CICLO')
cierre_ciclo.write.saveAsTable('STAGE_CIERRE_CICLO')


#%%   #################################################### FARMACIAS#####################################################################################
cierre_ciclo = Ra.select('cli_ciclo','cli_tipo','cli_linea','cli_distrito','cli_ruta','cli_repre')\
                 .where ( F.col('cli_tipo') == 'FARMACIA')\
                 .dropDuplicates()
cc = cierre_ciclo.alias('cc')
#%% territorio
repres = sparkSession.sql('SELECT * FROM STAGE_REPRESENTANTES')
a = cc.select('cli_ruta').join(repres, cc.cli_ruta== repres.rep_ruta,how='left').select(F.col('cli_ruta')\
                         .alias('rep_ruta'),'rep_territorio')\
                         .dropDuplicates()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.rep_ruta ,how='left').select('cc.*',F.col('rep_territorio').alias('territorio'))
cc = cierre_ciclo.alias('cc')
#%% plan_trabajo
a = Ta.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_frecuencia') != 0) &\
                                                 (F.col('cli_tipo') == ('FARMACIA')))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('plan_trabajo'))
cc = cierre_ciclo.alias('cc')
#%% medicos_visitas
a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('FARMACIA')))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('medicos_visitados'))
cc = cierre_ciclo.alias('cc')
#%% frecuencia 
a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('FARMACIA')) & \
                                                 (F.col(ciclo) == 1))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F0_1'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('FARMACIA')) & \
                                                 (F.col(ciclo) == 2))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F0_2'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('FARMACIA')) & \
                                                 (F.col(ciclo) >= 3))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F0_3'))
cc = cierre_ciclo.alias('cc')

cierre_ciclo = cierre_ciclo.withColumn('F1_1',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F1_2',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F1_3',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F2_1',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F2_2',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F2_3',F.lit(0))
cc = cierre_ciclo.alias('cc')
#%% COBERTURA
a = Tc.select('cob_ruta','cob_vis_registradas','cob_vis_objetivo','cob_vis_descontadas')\
      .filter((F.col('cob_tipo_cliente') == ('farmacia')))
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cob_ruta ,how='left')\
                 .select('cc.*',F.col('cob_vis_registradas').alias('visitas_reg'),\
                               F.col('cob_vis_objetivo').alias('visitas_obj'),\
                               F.col('cob_vis_descontadas').alias('visitas_desc'))
cc = cierre_ciclo.alias('cc')
#%%visitasacompañadas
a = Tb.select('vis_ruta','vis_acomp').filter((F.col('vis_acomp').isin(['GERENTE REGIONAL','Acompañado por Gerente'])) & \
                                                (F.col('vis_tipo_cliente') == ('farmacia')))\
                                     .groupby('vis_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.vis_ruta ,how='left')\
                 .select('cc.*',F.col('count').alias('visitas_acomp'))
cierre_ciclo = cierre_ciclo.fillna(0,subset=['F0_1','F0_2','F0_3','F1_1','F1_2','F1_3','F2_1','F2_2','F2_3',\
                                             'visitas_reg','visitas_obj','visitas_desc','visitas_acomp'])
cc = cierre_ciclo.alias('cc')


cierre_ciclo = cierre_ciclo.withColumn('plan_trabajo',cierre_ciclo['plan_trabajo'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('medicos_visitados',cierre_ciclo['medicos_visitados'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F0_1',cierre_ciclo['F0_1'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F0_2',cierre_ciclo['F0_2'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F0_3',cierre_ciclo['F0_3'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F1_1',cierre_ciclo['F1_1'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F1_2',cierre_ciclo['F1_2'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F1_3',cierre_ciclo['F1_3'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F2_1',cierre_ciclo['F2_1'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F2_2',cierre_ciclo['F2_2'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F2_3',cierre_ciclo['F2_3'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_reg',cierre_ciclo['visitas_reg'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_obj',cierre_ciclo['visitas_obj'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_desc',cierre_ciclo['visitas_desc'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_acomp',cierre_ciclo['visitas_acomp'].cast('Int'))

cierre_ciclo = cierre_ciclo.withColumn('cli_ruta_nombre',(F.concat(F.col('cli_ruta'), F.lit('/'), F.col('cli_repre'))))
cierre_ciclo.write.mode('append').saveAsTable('STAGE_CIERRE_CICLO')

#%%   ####################################################HOSPITAL#####################################################################################
cierre_ciclo = Ra.select('cli_ciclo','cli_tipo','cli_linea','cli_distrito','cli_ruta','cli_repre')\
                 .where ( F.col('cli_tipo') == 'HOSPITAL')\
                 .dropDuplicates()
cc = cierre_ciclo.alias('cc')
#%% territorio
repres = sparkSession.sql('SELECT * FROM STAGE_REPRESENTANTES')
a = cc.select('cli_ruta').join(repres, cc.cli_ruta== repres.rep_ruta,how='left').select(F.col('cli_ruta')\
                         .alias('rep_ruta'),'rep_territorio')\
                         .dropDuplicates()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.rep_ruta ,how='left').select('cc.*',F.col('rep_territorio').alias('territorio'))
cc = cierre_ciclo.alias('cc')
#%% plan_trabajo
a = Ta.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_frecuencia') != 0) &\
                                                 (F.col('cli_tipo') == ('HOSPITAL')))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('plan_trabajo'))
cc = cierre_ciclo.alias('cc')
#%% medicos_visitas
a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('HOSPITAL')))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('medicos_visitados'))
cc = cierre_ciclo.alias('cc')
#%% frecuencia 
a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('HOSPITAL')) & \
                                                 (F.col(ciclo) == 1))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F0_1'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('HOSPITAL')) & \
                                                 (F.col(ciclo) == 2))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F0_2'))
cc = cierre_ciclo.alias('cc')

a = Ra.select('cli_ruta','cli_clienteid').filter((F.col('cli_estatus').isin(['ACTIVO','NUEVO'])) & \
                                                 (F.col('cli_tipo') == ('HOSPITAL')) & \
                                                 (F.col(ciclo) >= 3))\
                                         .groupby('cli_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cli_ruta ,how='left').select('cc.*',F.col('count').alias('F0_3'))
cc = cierre_ciclo.alias('cc')

cierre_ciclo = cierre_ciclo.withColumn('F1_1',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F1_2',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F1_3',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F2_1',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F2_2',F.lit(0))
cierre_ciclo = cierre_ciclo.withColumn('F2_3',F.lit(0))
cc = cierre_ciclo.alias('cc')
#%% COBERTURA
a = Tc.select('cob_ruta','cob_vis_registradas','cob_vis_objetivo','cob_vis_descontadas')\
      .filter((F.col('cob_tipo_cliente') == ('hospital')))
cierre_ciclo = cc.join(a, cc.cli_ruta == a.cob_ruta ,how='left')\
                 .select('cc.*',F.col('cob_vis_registradas').alias('visitas_reg'),\
                               F.col('cob_vis_objetivo').alias('visitas_obj'),\
                               F.col('cob_vis_descontadas').alias('visitas_desc'))
cc = cierre_ciclo.alias('cc')
#%%visitasacompañadas
a = Tb.select('vis_ruta','vis_acomp').filter((F.col('vis_acomp').isin(['GERENTE REGIONAL','Acompañado por Gerente'])) & \
                                                (F.col('vis_tipo_cliente') == ('hospital')))\
                                     .groupby('vis_ruta').count()
cierre_ciclo = cc.join(a, cc.cli_ruta == a.vis_ruta ,how='left')\
                 .select('cc.*',F.col('count').alias('visitas_acomp'))
cierre_ciclo = cierre_ciclo.fillna(0,subset=['F0_1','F0_2','F0_3','F1_1','F1_2','F1_3','F2_1','F2_2','F2_3',\
                                             'visitas_reg','visitas_obj','visitas_desc','visitas_acomp'])
cc = cierre_ciclo.alias('cc')

cierre_ciclo = cierre_ciclo.withColumn('plan_trabajo',cierre_ciclo['plan_trabajo'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('medicos_visitados',cierre_ciclo['medicos_visitados'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F0_1',cierre_ciclo['F0_1'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F0_2',cierre_ciclo['F0_2'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F0_3',cierre_ciclo['F0_3'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F1_1',cierre_ciclo['F1_1'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F1_2',cierre_ciclo['F1_2'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F1_3',cierre_ciclo['F1_3'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F2_1',cierre_ciclo['F2_1'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F2_2',cierre_ciclo['F2_2'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('F2_3',cierre_ciclo['F2_3'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_reg',cierre_ciclo['visitas_reg'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_obj',cierre_ciclo['visitas_obj'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_desc',cierre_ciclo['visitas_desc'].cast('Int'))
cierre_ciclo = cierre_ciclo.withColumn('visitas_acomp',cierre_ciclo['visitas_acomp'].cast('Int'))

cierre_ciclo = cierre_ciclo.withColumn('cli_ruta_nombre',(F.concat(F.col('cli_ruta'), F.lit('/'), F.col('cli_repre'))))

cierre_ciclo.write.mode('append').saveAsTable('STAGE_CIERRE_CICLO')

sparkSession.stop()