from pyspark.sql import SparkSession
from pyspark.sql.functions import *



spark.conf.set("fs.s3a.access.key", "")
spark.conf.set("fs.s3a.secret.key", "")

def transform (df: DataFrame, col:"ColumnorName") -> Column:
   transform_df=df.withColumn( "weekday/weekend", when(date_format(col, 'EEE').isin(["Sat", "Sun"]),'Weekend').otherwise('Weekday'))\
    .withColumn('monthandyear', date_format(col,'MM/yyy'))\
    .withColumn('month', date_format(col,'MM'))\
    .withColumn('dayofweek', date_format(col, 'EEE'))\
    .withColumn('year', date_format(col,'yyy'))\
    .withColumns({'time': date_format(col,'HH:mm:ss'),'hour': date_format(col, 'HH')})\
    .withColumn('timeofday',when(date_format(col, 'HH')<12,'Morning').when(date_format(col, 'HH').between(12,17),'Afternoon').otherwise('Evening'))
   return transform_df

data_url = "s3://capstone-techcatalyst-raw/yellow_taxi/*"
df = spark.read.parquet(data_url)


df_yellow = df.withColumns({'trip_type': lit(-1), 'ehail_fee': lit(0), 'taxi_type': lit('yellow_taxi')})

df_yellow = df_yellow.dropDuplicates()

df_yellow_taxi = df_yellow

#### Extracting green_taxi Data from S3 ####
green_data_url = "s3://capstone-techcatalyst-raw/green_taxi/*"
df_green = spark.read.parquet(green_data_url)


df_green = df_green.withColumns({'taxi_type' : lit('green_taxi'), 'ehail_fee' : lit(0), 'Airport_fee' : lit(0)})


df_green_2023 = df_green.withColumnRenamed('lpep_pickup_datetime','tpep_pickup_datetime')
df_green_2023 = df_green_2023.withColumnRenamed('lpep_dropoff_datetime','tpep_dropoff_datetime')



df_green_2023 = df_green_2023.drop_duplicates()

df_green_taxi = df_green_2023



##### Combining Both Yellow and Green Taxi Data ######

alltaxi = df_yellow_taxi.unionByName(df_green_taxi, allowMissingColumns = True)

#alltaxi = alltaxi.drop('lpep_pickup_datetime')
alltaxi = transform(alltaxi,'tpep_pickup_datetime')

dates = ('2023','2024')

alltaxi = alltaxi.filter(alltaxi.year.between(*dates))


alltaxi= alltaxi.withColumn('trip_duration',(alltaxi.tpep_dropoff_datetime.cast('float') - alltaxi.tpep_pickup_datetime.cast('float'))/60)\
.withColumn('trip_duration', round('trip_duration' ,2))

alltaxi.display()
alltaxi.count()

### Writing to Conform bucket in S3 ###
output_path = f"s3a://capstone-techcatalyst-conformed/group_3_test/yellow_green_test"
alltaxi.write.partitionBy('year','month','taxi_type').parquet(output_path, mode = 'overwrite')

### writing to transform bucket in s3 ###
tf_output_path = f"s3a://capstone-techcatalyst-transformed/group_3_test/yellow_green_test"
alltaxi.write.partitionBy('year','month','taxi_type').parquet(tf_output_path, mode = 'overwrite')