import pyspark 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col 

S3_source_bucket = 's3://projecttest12345/data-source/weatherstats_vancouver_daily.csv'
S3_result_bucket = 's3://projecttest12345/data-result'

def weathertest():
    spark = SparkSession.builder.appName('Weatherdemo').getOrCreate()
    total_data = spark.read.csv(S3_source_bucket, header=True)
    print('Total number of records in the data source: %s' % total_data.count())
    result = total_data.where((col('avg_temperature') <= -5) & (col('snow') > 10)) 
    print('The days where the average temperature is colder than negative 5 celsius and snows more than 10 cm are: %s' % result.count())
    second_result = total_data.where((col('avg_relative_humidity') < 80) & (col('precipitation') < 2))
    print('The days where the average humidity is less than 80 % and the precipitation is less than 2 mm are: %s' % second_result.count())
    result.write.mode('overwrite').parquet(S3_result_bucket)
    print('Selected data was successfully saved to s3: %s' % S3_result_bucket)

if __name__ == '__main__':
    weathertest()

#vi weathertest.py
#spark-submit weathertest.py 