#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *


# In[ ]:


spark = SparkSession.builder.getOrCreate()


# In[ ]:


def process_log_data(source_path):

    # read log data file
    df = spark.read.json(raw_data_path, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create agg table
    agg_table = df.select("keyword","monthly_volume","cpc","dt").drop_duplicates()
    
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    
    agg_table = agg_table.withColumn("dt", get_timestamp("ts"))

    # extract columns to create time table
    time_table = agg_table.withColumn("hour",hour("start_time")).withColumn("day",dayofmonth("start_time")).withColumn("week",weekofyear("start_time")).withColumn("month",month("start_time")).withColumn("year",year("start_time")).withColumn("weekday",dayofweek("start_time")).select("ts","dt","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    return time_table

def write_prquet(df,output_path):
    
    df.write.parquet(os.path.join(output_path, "time_table/"), mode='overwrite', partitionBy=["day"])
    
    


# In[ ]:


def main():
    
    source_path = r"path"
    output_data = r"path"
    
    df = process_log_data(source_path)
    write_parquet(df,output_path)


# In[ ]:


if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:




