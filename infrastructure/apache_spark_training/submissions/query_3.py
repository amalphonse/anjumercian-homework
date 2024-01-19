#Joining the matches, match_details, medal_matches table using match_id 
bucketedValues = matches.alias("m").join(matchDetail.alias("md"), col("m.match_id") == col("md.match_id")) \
     .join(medal_matches.alias("mm"), col("m.match_id") == col("mm.match_id"))

#creating DDL for the joined table for bucket join
spark.sql("""DROP TABLE IF EXISTS bootcamp.matchdetailsmedals""")
bucketedDDL = """
 CREATE TABLE IF NOT EXISTS bootcamp.matchdetailsmedals (
    match_id STRING,
    mapid STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     completion_date TIMESTAMP,
     player_gamertag STRING,
     player_total_kills INTEGER,
     player_total_deaths INTEGER,
     medal_id BIGINT,
     count INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """

spark.sql(bucketedDDL)

#writing to matches, match_details, medals bucket joined tables
bucketedValues.select(
     col("m.match_id"), col("mapid"),col("is_team_game"), col("playlist_id"), col("completion_date"), \
    col("mm.player_gamertag"), col("player_total_kills"), col("player_total_deaths"), \
     col("medal_id"), col("count")
     ) \
     .write.mode("append")  \
     .bucketBy(16, "match_id").saveAsTable("bootcamp.matchdetailsmedals")
