
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  #I added the line to my spark code to disable the default behavior.

matches = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/matches.csv")
matchDetail =  spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/match_details.csv")
medals = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals.csv")
maps =  spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/maps.csv")
medal_matches =  spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals_matches_players.csv")

matches.createOrReplaceTempView("matches")
matchDetail.createOrReplaceTempView("match_details")
medals.createOrReplaceTempView("medals")
maps.createOrReplaceTempView("maps")
medal_matches.createOrReplaceTempView("medal_matches")
