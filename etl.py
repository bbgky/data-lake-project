import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id




config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load data from song_data dataset and extract columns
    for songs and artist tables and write the data into parquet
    files into s3.
    Parameters
    ----------
    spark: The created spark session     
    input_data: The path to the song_data s3 bucket.
    output_data: The path to store the parquet files.
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")
    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT DISTINCT
        song_id, title, artist_id, year, duration
    FROM
        songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id")\
                        .mode('overwrite').parquet(output_data + "/songs.parquet")



    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT
        artist_id, artist_name as name, 
        artist_location as location, 
        artist_latitude as lattitude, 
        artist_longitude as longitude
    FROM
        songs
    """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "/artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Load data from log_data dataset and extract columns
    for users and time tables, reads both the log_data and song_data
    datasets and extracts columns for songplays table with the data.
    It writes the data into parquet files into s3 bucket.
    Parameters
    ----------
    spark: The created spark session     
    input_data: The path to the song_data s3 bucket.
    output_data: The path to store the parquet files.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("logs")
    
    # filter by actions for song plays
    df = spark.sql("""
    SELECT *
    FROM
        logs
    WHERE
        page = 'NextSong'
    """)
    df.createOrReplaceTempView("logs")
    
    # extract columns for users table    
    users_table = spark.sql("""
    SELECT DISTINCT
        userid as user_id, 
        firstname as first_name, 
        lastname as last_name, 
        gender, 
        level
    FROM
        logs
    """)
    
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "/users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(int(x)/1000))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.timestamp))
    
    df.createOrReplaceTempView("logs")
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT DISTINCT
        datetime as start_time, 
        hour(datetime) as hour, 
        day(datetime) as day, 
        weekofyear(datetime) as week, 
        month(datetime) as month, 
        year(datetime) as year, 
        dayofweek(datetime) as weekday
    FROM
        logs
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month")\
                        .mode('overwrite').parquet(output_data + "/time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs.parquet")
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT
        datetime as start_time,
        userid as user_id,
        level,
        song_id,
        artist_id,
        sessionid as session_id,
        location,
        userAgent as user_agent,
        month(datetime) as month, 
        year(datetime) as year
    FROM
        logs
    JOIN
        songs 
    ON
        logs.song = songs.title
    """)
    songplays_table = songplays_table.withColumn("songplay_id",monotonically_increasing_id())
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month")\
                        .mode('overwrite').parquet(output_data + "/songplays.parquet")


def main():
    """
    Perform the following roles:
    1.) Create a spark session.
    2.) Process the raw song data and write parquet files into S3.
    3.) Process the raw event data and write parquet files into S3.
    """
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-project-hm"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
