# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as F
import pyspark.sql.types as T

SparkContext.setSystemProperty("hive.metastore.uris", "thrift://bdcsceprod-bdcsce-1.compute-590737110.oraclecloud.internal:9083") ##nn1:9083

#hive mestastore dir : /apps/hive/warehouse
sparkSession = (SparkSession.builder.appName('hive_connection').enableHiveSupport().getOrCreate())
sparkSession.sparkContext.setLogLevel("ERROR")

hive_command = sparkSession.sql('USE MEDIX')

#%% actualizar las tablas STAGE con respecto al ciclo
#CUP = sparkSession.read.option('header','true').csv('hdfs:///user/oracle/medix/auditoria/CUP/cup_medibutil_rev.csv')
#for x in list(range(1,16)):
#    CUP = CUP.withColumn('val'+str(x),CUP['val'+str(x)].cast('Int'))


#%% catalogo
catalogo = sparkSession.sql('SELECT * FROM STAGE_AUDITORIA_CATALOGO')
rutas = sparkSession.sql('SELECT * FROM EXT_TABLE_AUDITORIA_RUTAS')
Ct = catalogo.alias('Ct')
catalogo = catalogo.join(rutas,Ct.clo_brick_num == rutas.ar_brick_num,how='left')\
                   .select(F.col('clo_brick_num').alias('Brick_num'),\
                           F.col('clo_cdg_postal_num').alias('Cdg_postal'),\
                           F.col('clo_tipo_asent').alias('Tipo_asentamiento'),\
                           F.col('clo_estado').alias('Estado'),\
                           F.col('clo_municipio').alias('Municipio'),\
                           F.col('clo_colonia').alias('Colonia'),\
                           F.col('clo_ciudad').alias('Ciudad'),\
                           F.col('ar_linea').alias('Linea'),\
                           F.col('ar_ruta').alias('Ruta'))
Ct = catalogo.alias('Ct')
#%% CUP
CUP = sparkSession.sql('SELECT * FROM EXT_TABLE_AUDITORIA_CUP')
CUP= CUP.select('CUP_NOMBRE', 'CUP_DOMICILIO','CUP_LOCALIDAD','CUP_CDG_REGION','CUP_REGION',\
                'CUP_CDG_POSTAL','CUP_MATRICULA','CUP_CDG_ESP1','CUP_CDG_ESP2',\
                'CUP_ZONAPOSTAL','CUP_CATEGORIA',F.col('CUP_DESCRIPCION').alias('CUP_MERCADO'),'CUP_TOTAL_MERCADO',\
                'CUP_PERIODO','CUP_CDG_MEDICO','CUP_CDG_LIDER','CUP_MUNICIPIO','CUP_ID_UNICO',\
                'CUP_MATRICULA2','CUP_MATRICULA3','CUP_OTROSDOMICILIOS',
                F.concat(F.col('prod1'),F.lit('#'),F.col('val1') ).alias('Merge1'),\
                F.concat(F.col('prod2'),F.lit('#'),F.col('val2') ).alias('Merge2'),\
                F.concat(F.col('prod3'),F.lit('#'),F.col('val3') ).alias('Merge3'),\
                F.concat(F.col('prod4'),F.lit('#'),F.col('val4') ).alias('Merge4'),\
                F.concat(F.col('prod5'),F.lit('#'),F.col('val5') ).alias('Merge5'),\
                F.concat(F.col('prod6'),F.lit('#'),F.col('val6') ).alias('Merge6'),\
                F.concat(F.col('prod7'),F.lit('#'),F.col('val7') ).alias('Merge7'),\
                F.concat(F.col('prod8'),F.lit('#'),F.col('val8') ).alias('Merge8'),\
                F.concat(F.col('prod9'),F.lit('#'),F.col('val9') ).alias('Merge9'),\
                F.concat(F.col('prod10'),F.lit('#'),F.col('val10') ).alias('Merge10'),\
                F.concat(F.col('prod11'),F.lit('#'),F.col('val11') ).alias('Merge11'),\
                F.concat(F.col('prod12'),F.lit('#'),F.col('val12') ).alias('Merge12'),\
                F.concat(F.col('prod13'),F.lit('#'),F.col('val13') ).alias('Merge13'),\
                F.concat(F.col('prod14'),F.lit('#'),F.col('val14') ).alias('Merge14'),\
                F.concat(F.col('prod15'),F.lit('#'),F.col('val15') ).alias('Merge15'))

CUP = CUP.selectExpr('CUP_NOMBRE', 'CUP_DOMICILIO','CUP_LOCALIDAD','CUP_CDG_REGION','CUP_REGION',\
                'CUP_CDG_POSTAL','CUP_MATRICULA','CUP_CDG_ESP1','CUP_CDG_ESP2',\
                'CUP_ZONAPOSTAL','CUP_CATEGORIA','CUP_MERCADO','CUP_TOTAL_MERCADO',\
                'CUP_PERIODO','CUP_CDG_MEDICO','CUP_CDG_LIDER','CUP_MUNICIPIO','CUP_ID_UNICO',\
                'CUP_MATRICULA2','CUP_MATRICULA3','CUP_OTROSDOMICILIOS',\
                "stack(15,'Merge1',Merge1,'Merge2',Merge2,'Merge3',Merge3,'Merge4',Merge4,\
                'Merge5',Merge5,'Merge6',Merge6,'Merge7',Merge7,'Merge8',Merge8,'Merge9',Merge9,\
                'Merge10',Merge10,'Merge11',Merge11,'Merge12',Merge12,'Merge13',Merge13,\
                'Merge14',Merge14,'Merge15',Merge15) as (CUP_POSICION,CUP_PRODUCTO)")\
       .where('CUP_PRODUCTO is not null')

spl= F.split(CUP['CUP_PRODUCTO'], '#')
CUP = CUP.withColumn('CUP_VALUE',spl.getItem(1).cast(T.IntegerType()) )
CUP = CUP.withColumn('CUP_PRODUCTO',spl.getItem(0))
spl= F.split(CUP['CUP_POSICION'], 'e')
CUP = CUP.withColumn('CUP_POSICION',spl.getItem(2).cast(T.IntegerType()) )
CUP =CUP.withColumn('CUP_ANO',CUP.CUP_PERIODO.substr(-2,2))
CUP =CUP.withColumn('CUP_MES',CUP.CUP_PERIODO.substr(-5,2).cast(T.IntegerType()))
CUP =CUP.withColumn('CUP_MES',F.when(CUP.CUP_MES==1,'Ene').when(CUP.CUP_MES==2,'Feb').when(CUP.CUP_MES==3,'Mar')\
                               .when(CUP.CUP_MES==4,'Abr').when(CUP.CUP_MES==5,'May').when(CUP.CUP_MES==6,'Jun')\
                               .when(CUP.CUP_MES==7,'Jul').when(CUP.CUP_MES==8,'Ago').when(CUP.CUP_MES==9,'Sep')\
                               .when(CUP.CUP_MES==10,'Oct').when(CUP.CUP_MES==11,'Nov').when(CUP.CUP_MES==12,'Dic')\
                               .otherwise(CUP.CUP_MES))

###################AGREGAR TODOS LOS MERCADOS DISPONIBLES######################
CUP = CUP.withColumn('CUP_MERCADO',F.when(CUP.CUP_MERCADO=='MDO MEDIBUTIN V3.0','MDO MEDIBUTIN')\
                                    .when(CUP.CUP_MERCADO=='MDO A08A ANOREX V3.1','MDO ANOREXIGENICOS'))
###################AGREGAR TODOS LOS PRODUCTOS DISPONIBLES######################
CUP = CUP.withColumn('CUP_PRODUCTO',F.when(CUP.CUP_PRODUCTO == 'ESPAVEN ALCALINO VLT', 'ESPAVEN ALCALINO (VLT)')\
                                      .when(CUP.CUP_PRODUCTO == 'GELAN PLUS CHN', 'GELAN PLUS (CHN)')\
                                      .when(CUP.CUP_PRODUCTO == 'GEX RIM', 'GEX (RIM)')\
                                      .when(CUP.CUP_PRODUCTO == 'GRALOXEN MVI', 'GRALOXEN (MVI)')\
                                      .when(CUP.CUP_PRODUCTO == 'MEDIBUTIN MDX', 'MEDIBUTIN (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'PRAMIGEL CAT', 'PRAMIGEL (CAT)')\
                                      .when(CUP.CUP_PRODUCTO == 'RIOPAN TKD', 'RIOPAN (TAK)')\
                                      .when(CUP.CUP_PRODUCTO == 'ACXION C IFS', 'ACXION C (IFS)')\
                                      .when(CUP.CUP_PRODUCTO == 'ASENLIX SAN', 'ASENLIX (SAN)')\
                                      .when(CUP.CUP_PRODUCTO == 'ESBELCAPS MDX', 'ESBELCAPS (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'EZBENCX L7R', 'EZBENCX (L7R)')\
                                      .when(CUP.CUP_PRODUCTO == 'IFA LOSE IFS', 'IFA-LOSE (IFS)')\
                                      .when(CUP.CUP_PRODUCTO == 'IFA NOREX IFS', 'IFA-NOREX (IFS)')\
                                      .when(CUP.CUP_PRODUCTO == 'IFA-ACXION AP IFS', 'ACXION AP (IFS)')\
                                      .when(CUP.CUP_PRODUCTO == 'IFA-ACXION IFS', 'ACXION (IFS)')\
                                      .when(CUP.CUP_PRODUCTO == 'IFA-ITRAVIL AP IFS', 'ITRAVIL AP (IFS)')\
                                      .when(CUP.CUP_PRODUCTO == 'IFA-ITRAVIL IFS', 'ITRAVIL (IFS)')\
                                      .when(CUP.CUP_PRODUCTO == 'IFA-REDUCCING S IFS', 'IFA-REDUCCING S (IFS)')\
                                      .when(CUP.CUP_PRODUCTO == 'MZ1 MDX', 'MZ1 (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'NEOBES MDX', 'NEOBES (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'OBECLOX LP MDX', 'OBECLOX LP (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'OBECLOX MDX', 'OBECLOX (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'REDICRES CUB', 'REDICRES (CUB)')\
                                      .when(CUP.CUP_PRODUCTO == 'REDOTEX MDX', 'REDOTEX (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'REDOTEX NF MDX', 'REDOTEX NF (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'SOLUCAPS MDX', 'SOLUCAPS (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'TERFAMEX MDX', 'TERFAMEX (MDX)')\
                                      .when(CUP.CUP_PRODUCTO == 'TERFAMEX OD MDX', 'TERFAMEX OD (MDX)')\
                                      .otherwise(CUP.CUP_PRODUCTO))

CUP = CUP.withColumn('CUP_ANO',CUP['CUP_ANO'].cast('Int'))
CUP = CUP.withColumn('CUP_CDG_POSTAL',CUP['CUP_CDG_POSTAL'].cast('Int'))

#%% DDD
DDD = sparkSession.read.option('header','true').csv('hdfs:///user/oracle/medix/auditoria/DDD/unidades/ddd_rev_201805.csv')

name ="stack(24,"+','.join(['\''+ x + '\','+ x for x in DDD.schema.names[17:41]])+") as (DDD_MES, DDD_CANTIDAD)"
DDD= DDD.selectExpr('DDD_MERCADO','DDD_PRODUCTO_HOM','DDD_TIPO_PROD','DDD_PARRILLA','DDD_LABORATORIO','DDD_LINEA',\
                  'DDD_TERRITORIO','DDD_VISITADO','DDD_GTR','DDD_CDG_POSTAL','DDD_BRICK','DDD_CDG_BRICK','DDD_DESC_BRICK',\
                  'DDD_MUNICIPIO','DDD_ESTADO','DDD_CDG_PACK','DDD_PRODUCTO','DDD_CICLO',\
                  name)\
        .where('DDD_CANTIDAD is not null')
DDD = DDD.withColumn('DDD_ANO',DDD.DDD_MES.substr(4,2))
DDD = DDD.withColumn('DDD_MES',DDD.DDD_MES.substr(0,3))

DDD = DDD.withColumn('DDD_MERCADO',F.when(DDD.DDD_MERCADO=='MDO A08A ANOREXIGENICOS','MDO ANOREXIGENICOS')\
                                    .otherwise(DDD.DDD_MERCADO))

DDD = DDD.withColumn('DDD_CDG_BRICK',DDD['DDD_CDG_BRICK'].cast('Int'))
DDD = DDD.withColumn('DDD_ANO',DDD['DDD_ANO'].cast('Int'))
DDD = DDD.withColumn('DDD_CANTIDAD',DDD['DDD_CANTIDAD'].cast('Int'))

#%%  MERCADO MEDIBUTIN 
linea='GAS'
mercado= 'MDO MEDIBUTIN'
ctg= catalogo.select(catalogo.columns).filter(F.col('Linea') == linea)\
             .groupBy('Brick_num','Cdg_postal','Estado','Municipio','Ciudad','Linea','Ruta').count()
cr = CUP.select('CUP_ANO','CUP_MES','CUP_CDG_POSTAL','CUP_MERCADO','CUP_PRODUCTO','CUP_POSICION','CUP_VALUE')\
        .groupBy('CUP_ANO','CUP_MES','CUP_CDG_POSTAL','CUP_MERCADO','CUP_POSICION','CUP_PRODUCTO').sum('CUP_VALUE')\
       .filter((F.col('CUP_MERCADO') == mercado ) )\
       .join(ctg, CUP.CUP_CDG_POSTAL == ctg.Cdg_postal, how = 'left')\
       .select('Brick_num','Cdg_postal','Estado','Municipio','Ciudad','Linea','Ruta',\
               'CUP_ANO','CUP_MES','CUP_MERCADO','CUP_PRODUCTO','CUP_POSICION',F.col('sum(CUP_VALUE)').alias('CUP_VALUES'))
cr = cr.withColumn('CUP_VALUES',cr['CUP_VALUES'].cast('Int'))

dr= DDD.select('DDD_CDG_BRICK','DDD_MERCADO','DDD_PRODUCTO_HOM',\
               'DDD_TERRITORIO','DDD_ANO','DDD_MES','DDD_CANTIDAD')\
       .groupBy('DDD_CDG_BRICK','DDD_MERCADO','DDD_PRODUCTO_HOM',\
               'DDD_TERRITORIO','DDD_ANO','DDD_MES','DDD_CANTIDAD').count()\
       .filter((F.col('DDD_MERCADO')== mercado))\
       .groupBy('DDD_CDG_BRICK','DDD_MERCADO','DDD_PRODUCTO_HOM', \
                'DDD_TERRITORIO','DDD_ANO','DDD_MES')\
       .sum('DDD_CANTIDAD')
dr = dr.withColumn('sum(DDD_CANTIDAD)',dr['sum(DDD_CANTIDAD)'].cast('Int'))

c = cr.join(dr, [cr.Brick_num ==dr.DDD_CDG_BRICK ,cr.CUP_PRODUCTO == dr.DDD_PRODUCTO_HOM ,\
                 cr.CUP_ANO == dr.DDD_ANO , cr.CUP_MES == dr.DDD_MES ], how='left')\
     .select('Brick_num','Cdg_postal','Estado','Municipio','Ciudad','Linea','Ruta',\
              'CUP_ANO','CUP_MES','CUP_MERCADO',F.col('CUP_PRODUCTO').substr(-4,3).alias('Laboratorio'),\
              'CUP_PRODUCTO','CUP_POSICION','CUP_VALUES',\
              F.col('sum(DDD_CANTIDAD)').alias('DDD_TOTAL'))

hive_command = sparkSession.sql('DROP TABLE IF EXISTS STAGE_AUDITORIA_CUPDDD')
c.coalesce(1).write.saveAsTable('STAGE_AUDITORIA_CUPDDD')
sparkSession.stop()

