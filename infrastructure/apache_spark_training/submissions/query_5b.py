second_sort_df = bucketedValues.repartition(10, col("completion_date")) \
        .sortWithinPartitions(col("match_id")) \
        .withColumn("completion_date", col("completion_date").cast("timestamp")) 


third_sort_df = bucketedValues.repartition(10, col("event_date")) \
        .sortWithinPartitions(col("event_date")) \
        .withColumn("completion_date", col("completion_date").cast("timestamp"))
