# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Task 1: User Favorite Genres
genre_counts = listening_logs_df.join(songs_metadata_df, "song_id").groupBy("user_id", "genre").agg(count("*").alias("user_genre_count"))
window = Window.partitionBy("user_id").orderBy(desc("user_genre_count"))
favorite_genres = genre_counts.withColumn("rank", dense_rank().over(window)).filter("rank = 1").select("user_id", "genre", "user_genre_count").show()

# Task 2: Average Listen Time
average_listening_time = listening_logs_df.groupBy("user_id").agg(avg("duration_sec").alias("Average Listen Time")).show()

# Task 3: Create your own Genre Loyalty Scores and rank them and list out top 10
total_listens = genre_counts.groupBy("user_id").agg(sum("user_genre_count").alias("total_listens"))
loyalty_score = favorite_genres.join(total_listens, "user_id").withColumn("loyalty_score", col("user_genre_count") / col("total_listens")).select("user_id", "genre", "loyalty_score")
top_loyal_users = loyalty_score.orderBy(desc("loyalty_score")).limit(10).show()

# Task 4: Identify users who listen between 12 AM and 5 AM
users_listen = listening_logs_df.select("user_id").where(hour("timestamp").between(0, 5)).distinct().show()