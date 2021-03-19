import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main(spark):

    df_X = spark.read.csv('X1_1.csv')
    df_t = spark.read.csv('t1.csv')

    # Converting all DF columns to float
    for col in df_t.columns:
        df_t = df_t.withColumn(col, df_t[col].cast('float'))
    for col in df_X.columns:
        df_X = df_X.withColumn(col, df_X[col].cast('float'))

    # Trying to dot both matrices but it did not work :(
    df = spark.createDataFrame(df_X.toPandas().T.dot(df_t.toPandas()))
    #df_X.head()
    #df_t.head()

if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())