
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, avg
from pyspark.sql import Window
from pyspark.sql import functions as F
import os
spark = SparkSession.builder.getOrCreate()


def process_log_data(source_path):
    # read log data file
    df = spark.read.json(source_path, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    #Split  dt Column using withColumn() to  year month and day based on date pattern
    df = df.withColumn('year', split(df['dt'], '-').getItem(0)).withColumn('month', split(df['dt'], '-').getItem(1)).withColumn('day', split(df['dt'], '-').getItem(2))
    df.show()
    #partition by day the df
    windowSpecAgg = Window.partitionBy("day")

    df = df.withColumn("avg_searches_monthly_volume", avg(col("monthly_volume")).over(windowSpecAgg)).withColumn("avg_cpc", avg(col("cpc")).over(windowSpecAgg)).select("keyword", "avg_searches_monthly_volume","avg_cpc", "dt","year","month","day")

    return df

def write_prquet(df, output_path):

    df = df.withColumn("dt", F.concat(F.col("year"),F.col("month"),F.col("day")))
    df = df.select("keyword", "avg_searches_monthly_volume", "avg_cpc", "dt")
    df.show()
    df.write.parquet(os.path.join(output_path, "aggregated/"), mode='overwrite', partitionBy=["dt"])

def main():

    source_path = r"path"
    output_data = r"path"

    df = process_log_data(source_path)
    write_prquet(df, output_data)

if __name__ == "__main__":
    main()

## question 2 -

# to prevent loading same file more then once I will use os.path.exists() function to check if a file exists before loading

# to make sure that my pipeline is working well I will create test data - expected result some small dataset (afew rows)
# will loop through row by row and compare my results to the expected data.