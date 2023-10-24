from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType, ArrayType,IntegerType
from pyspark.sql.functions import col, split, explode, row_number, trim, monotonically_increasing_id, col, expr,udf, concat_ws, when
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.functions import lit
spark = SparkSession.builder.getOrCreate()



#Creamos las rutas de lectura y escritura
bucket_name = 'gs://dmc-proyecto-big-data-24' 

ruta_workload_distritos= f"{bucket_name}/datalake/workload/FastAPI/csv/DistritosPeru/distritos-peru.csv"


ruta_landing_distritos= f"{bucket_name}/datalake/landing/distritos_peru/"



#Definimos el schema 
df_schema = StructType([
    StructField("geolocalizacion",StringType(),False),
    StructField("coddepartamento",StringType(),False),
    StructField("departamento",StringType(),False),
    StructField("codprovinvia",StringType(),False),
    StructField("provinvia",StringType(),False),
    StructField("coddistrito",StringType(),False),
    StructField("distrito",StringType(),False),
    StructField("capital",StringType(),True),
    StructField("ubigeo",StringType(),True),
    StructField("idprov",StringType(),True),
    StructField("codigo",StringType(),True),
    StructField("cntcpp",StringType(),True),
    StructField("desdistrito",StringType(),True),
    StructField("poblacion",IntegerType(),True),
    StructField("fechadata",IntegerType(),True),
    StructField("origdata",StringType(),True),    
])
dfdistritos_peru= spark.read.csv(ruta_workload_distritos,header = True,schema = df_schema,encoding="ISO-8859-1",sep = ";")
dfdistritos_peru.show()


dffinal = dfdistritos_peru.withColumn("fecactualizacion", lit(datetime.today()))
dffinal.show(10)

dffinal.repartition(2).write.partitionBy("coddepartamento").mode("overwrite").format("parquet").option("partitionOverwriteMode", "dynamic").option("header","true").save(ruta_landing_distritos)


spark.stop()

exit()