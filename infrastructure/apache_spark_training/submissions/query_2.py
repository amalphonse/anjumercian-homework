explicitBroadcast = bucketedValues.alias("bv").join(broadcast(maps).alias("mp"), col("bv.mapid") == col("mp.mapid")) \
                .join(broadcast(medals).alias("md"), col("bv.medal_id") == col("md.medal_id"))

#query 2 explicit broadcast between medals and maps
