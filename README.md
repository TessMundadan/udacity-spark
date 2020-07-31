#Context


Music streaming app wants to analyze the what songs users are listening to.Read input json data from S3, Use spark to process large data and
write the tables back to S3


#Database Schema Design

First we read the data from S3.With spark we process data by using dataframes and do not create tables explicilty.The ouput is written back to S3 as parquet files<br>
We used a star schema.The dimension tables have details of each dimension. Fact table contain the ids of the dimension table and the metrics which are being analyzed<br>


Dimension Tables<br>
users<br>
songs<br>
artists<br>
Time<br>

Fact table<br>
Songsplay


#ETL Pipeline Design

process_song_data()<br>
Reads songs JSON  files from the S3 and write songs and artists parquet files back to S3.<br>

process_log_data()<br>
Reads logs JSON  files from the S3 and write users,time and songplay parquet files back to S3.<br>
