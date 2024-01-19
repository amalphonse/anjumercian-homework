#Joining the matches, match_details, medal_matches table using match_id 
bucketedValues = matches.alias("m").join(matchDetail.alias("md"), col("m.match_id") == col("md.match_id")) \
     .join(medal_matches.alias("mm"), col("m.match_id") == col("mm.match_id"))
