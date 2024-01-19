second_sort_df = bucketedValues.sortWithinPartitions(col("mapid")) 

second_sort_df.count()
6885858

