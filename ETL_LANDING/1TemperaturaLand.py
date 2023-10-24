from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType, ArrayType
#from pyspark.sql.functions import col, split, explode, row_number, trim, monotonically_increasing_id, col, expr,udf, concat_ws, when
from pyspark.sql.window import Window
import h5netcdf
#from functools import reduce
import netCDF4 as nc
import xarray as xr
import fsspec
#import glob 
#Creamos las session de apache spark en una variable

spark = SparkSession.builder.getOrCreate()


def main():
    list_variable =["cloud_cover"]
    list_mounth = ["01","02","03","04","05","06","07","08","09","10","11","12"]
    list_year = ["2019","2020","2021","2022","2023"]
    #list_variable = ["cloud_cover","snow_thickness_lwe","2m_temperature","2m_dewpoint_temperature","snow_thickness","vapour_pressure","10m_wind_speed"]
    #list_mounth = ["01","02","03","04","05","06","07","08","09","10","11","12"]
    #list_year = ["2018","2019","2020","2021","2022","2023"]
    for variable in list_variable:
        for year in list_year:
            for mounth in list_mounth:
                rutas_ejecucion(mounth, year, variable)                
    return ()
    
    
    
#Creamos las rutas de lectura y escritura
def rutas_ejecucion(mounth, year, variable):    
    name_file = f"1CDSAPI-{variable}-{year}-{mounth}.nc"
    bucket_name = 'gs://dmc-proyecto-big-data-24' 
    ruta_wrkld_cds_cloud_cover= f"{bucket_name}/datalake/workload/cdsapi/{variable}/{name_file}"
    ruta_ldng_cds_cloud_cover= f"{bucket_name}/datalake/landing/var_metereologicas/{variable}/"
    
    dataset = load_dataset(ruta_wrkld_cds_cloud_cover)
    dfdropna = convert_dataframe(dataset)
    #print(dfdropna)
    sparkDF = spark.createDataFrame(dfdropna)
    sparkDF.repartition(2).write.partitionBy("time").mode("overwrite").format("parquet").option(
    "partitionOverwriteMode", "dynamic").save(ruta_ldng_cds_cloud_cover)
    return sparkDF
    
    
    
def load_dataset(filename, engine="h5netcdf", *args, **kwargs) -> xr.Dataset:
    """Load a NetCDF dataset from local file system or cloud bucket."""
    with fsspec.open(filename, mode="rb") as file:
        dataset = xr.load_dataset(file, engine=engine, *args, **kwargs)
    return dataset


def convert_dataframe(dataset):
    df = dataset.to_dataframe()
    #print(df.columns)
    dfi = df.reset_index()
    dfdropna = dfi.dropna()
    return dfdropna

main()

spark.stop()

exit()