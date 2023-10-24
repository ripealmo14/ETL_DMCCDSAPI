from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType, ArrayType,IntegerType
#from pyspark.sql.functions import col, split, explode, row_number, trim, monotonically_increasing_id, col, expr,udf, concat_ws, when
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.functions import lit
spark = SparkSession.builder.getOrCreate()


#Creamos las rutas de lectura y escritura
bucket_name = 'gs://dmc-proyecto-big-data-24' 

ruta_landing_mmvar= f"{bucket_name}/datalake/landing/des_var_meteorologicas/"

ruta_curated_mmvar= f"{bucket_name}/datalake/curated/mm_des_var/"

dfvarcurated = spark.read.format("parquet").option("header","true").load(ruta_landing_mmvar)
dfvarcurated.show()