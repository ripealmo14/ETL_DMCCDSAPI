from pyspark.sql import SparkSession
from pyspark.sql.types import  FloatType
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
import pyspark.sql.functions as sql
import time
#from pyspark.sql.window import Window



#Creamos las session de apache spark en una variable

spark = SparkSession.builder.getOrCreate()


def main():
    list_codmes = ["202301","202302","202303","202304","202305","202306","202307","202308","202309","202310"]
    #list_variable = ["cloud_cover","snow_thickness_lwe","2m_temperature","2m_dewpoint_temperature","snow_thickness","vapour_pressure","10m_wind_speed"]
    #list_mounth = ["01","02","03","04","05","06","07","08","09","10","11","12"]
    #list_year = ["2018","2019","2020","2021","2022","2023"]
    for codmes in list_codmes:
        ejecucion(codmes)
        time.sleep(30)
    return ()
    
    
    
#Creamos las rutas de lectura y escritura
def ejecucion(codmes):
    bucket_name = 'gs://dmc-proyecto-big-data-24' 
    ruta_ldng_var_metereologicas = f"{bucket_name}/datalake/landing/var_metereologicas/temperatura_rocio/"
    ruta_curated_var_metereologicas= f"{bucket_name}/datalake/curated/var_metereologicas/his_var_metereologica/"
    cons_codmes = codmes
    ruta_curated_distritos= f"{bucket_name}/datalake/curated/mm_distritos_peru/"
    
    dfFinal = transformacion(ruta_ldng_var_metereologicas,ruta_curated_distritos,cons_codmes)
    dfFinal.show()
    #print(dfFinal.count())
    #dfFinal.printSchema()
    dfFinal.repartition(2).write.partitionBy("codigovar","CODMES").mode("overwrite").format("parquet").option("partitionOverwriteMode", "dynamic").save(ruta_curated_var_metereologicas)
    return ()
    
def transformacion(ruta,ruta_dist,codmes):
    dfvarcurated = spark.read.format("parquet").option("header","true").load(ruta)
    df1 = dfvarcurated.withColumn("CODMES", sql.concat(col('time').substr(1,4),col('time').substr(6,2)))
    df1filter = df1.filter(col("CODMES") == codmes)

    df2 = df1filter.withColumn("latitud",col("lat").cast(FloatType()))\
        .withColumn("longitud",col("lon").cast(FloatType()))\
        .withColumn("valor_var",col("Dew_Point_Temperature_2m_Mean").cast(FloatType()))
    
    df3 = df2.withColumn("latitud", lit(sql.round("latitud", scale=2)))\
            .withColumn("longitud", lit(sql.round("longitud", scale=2)))\
            .withColumn("valor_var", lit(sql.round("valor_var", scale=2)))

    df4 = df3.drop('lat','lon','time','Dew_Point_Temperature_2m_Mean')
    
    df5 = df4.withColumn("codigovar", lit("TEPRO"))

    df1distritos = spark.read.format("parquet").option("header","true").load(ruta_dist)

    df1distritos.createOrReplaceTempView("tb_distritos")
    df5.createOrReplaceTempView("tb_historica")

    dfresult = spark.sql("SELECT H.CODMES, H.codigovar, H.valor_var, D.ubigeo FROM tb_historica H LEFT JOIN tb_distritos D ON H.latitud = D.latitud and H.longitud = D.longitud ORDER BY H.CODMES desc")
    dffinal = dfresult.dropna()
    return (dffinal)



    


main()

spark.stop()

exit()