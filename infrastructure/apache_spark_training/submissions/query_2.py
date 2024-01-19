
#join the medals and maps tables with an explicitly specified a broadcast join (query_2)
explicitBroadcast = medals.alias("md").join(broadcast(maps).alias("mp"), col("md.mapid") == col("mp.mapid"))
