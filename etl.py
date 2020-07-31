import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.types import TimestampType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import from_unixtime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['Default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
#    """
#
#   The purpose of this function is to create spark session
#
#    Args:
#    None
#
#    Returns:
#    spark: spark session.
#
#    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
#    """
#
#   The purpose of this function is to read song json files from S3 bucket create song table and artist   
#   table and write back to S3.
#
#    Args:
#    spark: spark session.
#    input_output: S3 bucket for input data.
#    output_output: S3 bucket for output data.
#
#    Returns:
#    None
#
#    """
    # get filepath to song data file
    song_data_read_path = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file
    song_df = spark.read.json(song_data_read_path)
   
   

    # extract columns to create songs table
    songs_table = song_df.select("song_id","title","artist_id","year","duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs_table'),'overwrite')

    # extract columns to create artists table
    artists_table = song_df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").dropDuplicates()

    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists_table'),'overwrite')



def process_log_data(spark, input_data, output_data):
#    """
#
#   The purpose of this function is to read log json files from S3 bucket create user table,time and songplay parquet files
#   and write back to S3.
#
#    Args:
#    spark: spark session.
#    input_output: S3 bucket for input data.
#    output_output: S3 bucket for output data.
#
#    Returns:
#    None
#
#    """
    # get filepath to log data file
    log_data_read_path = os.path.join(input_data, "log-data/*/*/*.json")


    # read log data file
    log_df = spark.read.json(log_data_read_path)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')


    # extract columns for users table    
    users_table = log_df.select("userId","firstName","lastName","gender","level").dropDuplicates()

    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users_table'),'overwrite')

    # create timestamp column from original timestamp column

    get_timestamp = udf(lambda x: x/1000,DoubleType())
    log_df = log_df.withColumn('start_time', get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
 
    get_datetime = udf(lambda x: from_unixtime(x))
    log_df = log_df.withColumn('datetime', from_unixtime(col('start_time')))

    # extract columns to create time table
    time_table = log_df.select('start_time',\
              hour(col('datetime')).alias('hour'),\
              dayofmonth(col('datetime')).alias('day'),\
              weekofyear(col('datetime')).alias('week'),\
              month(col('datetime')).alias('month'),\
              year(col('datetime')).alias('year'),\
              date_format(col('datetime'),"EEEE").alias('weekday'),\
             ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'time_table'),'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data,'songs_table'))
    time_df = spark.read.parquet(os.path.join(output_data,'time_table'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df,[log_df.song == song_df.title,log_df.length == song_df.duration], how='inner')\
                   .join(time_df,log_df.start_time == time_df.start_time,how='inner')\
                   .dropDuplicates()\
                   .withColumn("songplay_id", monotonically_increasing_id())\
                   .select("songplay_id",time_df.start_time,log_df.userId.alias('user_id'),log_df.level,song_df.song_id,\
                    song_df.artist_id,log_df.sessionId.alias('session_id'),log_df.location,log_df.userAgent.alias('user_agent'),\
                    time_df.year,time_df.month)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'songplays_table'),'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-udacity/"
   
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
