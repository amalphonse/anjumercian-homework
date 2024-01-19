first_sort_df = bucketedValues.sortWithinPartitions(col("match_id"), col("completion_date"), col("mapid"))
