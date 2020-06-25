# data-lake-project
Udacity Data Engineer Nano Degree data lake project


## Context:

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Raw data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. An ETL pipeline is going to be implemented for analytical use cases. The pipeline extracts the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

## Analytical goals:

The analytics team is particularly interested in understanding what songs users are listening to.

## Database schema design:

### Fact Table

1. **songplays** - records in log data associated with song plays i.e. records with page `NextSong`.

	* songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent

### Dimension Tables

2. **users** - users in the app

	* user\_id, first\_name, last\_name, gender, level
	
3. **songs** - songs in music database
	* song\_id, title, artist\_id, year, duration

4. **artists** - artists in music database
	* artist_id, name, location, latitude, longitude
5. **time** - timestamps of records in songplays broken down into specific units
	* start\_time, hour, day, week, month, year, weekday

## ETL pipeline

**Step 0:** 

Setup aws configurations using `aws_access_key_id` and `aws_secret_access_key`. 

**Step 1:** 

Create a spark session using `org.apache.hadoop:hadoop-aws:2.7.0`


**Step 2:** 

Process raw song json data. Extract data for songs and artists table; write the two tables into S3 in the format of parquet.

**Step 3:** 

Process raw events json data. Extract data for users and time table; join the log events data with the song table in the previous step, to get data for songplay table; write the three tables into S3 in the format of parquet.

## How to run the code

1. Replace AWS Credentials in dl.cfg.
2. Modify input and output data paths in etl.py main function.
3. In the terminal, under the main directory, run `python etl.py`

