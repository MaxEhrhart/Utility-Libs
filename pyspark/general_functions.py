from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

hc = HiveContext(sc)

def replace_columnnames_chars(df,to_replace=" ",replace_to="_"):
    """
        Replaces characters in column names. 
        Parameters:
            df: dataframe.
            to_replace: char in column name to replace.
            replace_to: char in column name to be replaced.
        Return: Renamed columns dataframe.
    """
    columns = [col.replace(to_replace,replace_to) for col in df.columns]
    for index in range(len(df.columns)):
        df = df.withColumnRenamed(df.columns[index], columns[index])
    return df