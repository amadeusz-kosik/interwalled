# Interval join helper for Apache Spark
This repository contains all components to implement, use and benchmark a few implementations for interval join 
    in Apache Spark library. The interval join is defined as a join with following conditions 
    on _database_ and _query_ tables:
```sql
SELECT 
    *
FROM 
    database, query
WHERE database.key = query.key -- grouping key
    AND query.start    <= database.end
    AND database.start <= query.end
```

## Join algorithm
This library uses algorithms and data structures based on Augmented Interval List (AIList) data structure to store 
    and effectively join intervals in SQL join. The relevant paper is published
    [on biorxiv.org](https://www.biorxiv.org/content/10.1101/593657v1).

## Implementation details
Following descriptions assume joining bigger _database_ dataset with smaller _query_ one.

### Bucketing
Some of the algorithms are prefixed with _Partitioned_ keyword meaning that data is bucketed before joining. Bucketing
    algorithm is implemented in `Bucketizer` class and is used to split long partitions into smaller ones. It takes
    _bucketScale_ parameter and:
1. Accepts the intervals' dataset.
2. For each interval:
   1. Scales down _to_ and _from_ columns by _bucketScale_ factor.
   2. For range of _i_ in [_from_ / _bucketScale_; _to_ / _bucketScale_]: creates copy of interval, assigns _i_ bucket 
        to it.
3. Calls repartitioning by both _bucket_ and _key_ columns.

#### Example:
Input: _bucketScale_ := 10
```csv
key, from, to, value
CH1     2,  4     A1
CH1     7, 11     A2
CH1    12, 14     A3
```

```csv
key, from, to, value
CH2     1, 21     B1
```

Output:
```csv
key, from, to, bucket, value
CH1     2,  4,      0     A1
CH1     7, 11,      0     A2
CH1     7, 11,      1     A2
CH1    12, 14,      1     A3
```

```csv
key, from, to, bucket, value
CH2     1, 21,      0     B1
CH2     1, 21,      1     B1
CH2     1, 21,      2     B1
```

This transformation allows to call Spark's _.cogroup_ function on smaller partitions (using _key_ and _bucket_ instead
    of only the former one). Call to _.cogroup_ function is necessary to perform the join and is part of the actual 
    join algorithm. After handling the join, the algorithm must also call _.distinct_ to get rid of duplicated rows
    (in above example: A2-B1 pair would appear two times).

### BroadcastAIList
This is the reference implementation:
1. Collect the _query_ onto driver.
2. Compute AIList.
3. Broadcast query onto executors.
4. Perform the join in parallel on executors.

### NativeAIList
This implementation is similar to _BroadcastPartitionedAIList_, but instead of computing AILists in Java code, it uses
    built-in native Spark functions (grouping and windowing) to create the list.

1. Compute the AIList components on sorted _database_ dataset - iteratively:
   1. For each interval not assigned to a component, compute how many following elements it covers.
   2. If the interval covers more elements that a threshold, mark it as unassigned, otherwise assign it to
        the being currently built component.
   3. Mark component as completed. Go back if there are more components to be built or stop if either no intervals
        were unassigned or maximum number of components is reached.
2. Use windowing to compute _Max E_ (max end) values for each row.
3. Cogroup _query_ with _database_.
4. Perform the join - for each AIList skip rows where _MAX E_ is lower that joining interval, take until AIList rows
    begin to be greater than the query ends, filter out only true matches.

```scala
aiList
  .dropWhile(row => row.getAs[Long](_MAX_E) < queryFrom)
  .takeWhile(row => row.getAs[Long](FROM) <= queryTo)
  .filter(row => row.getAs[Long](TO) >= queryFrom)
```

This implementation does not take hits from GC, but it is prone to data skew and offers slightly worse performance than
    the Broadcast one. Also, being an iterative job, it heavily relies on caching and might benefit from storing the
    iterations' results instead of recomputing them.

### PartitionedAIList
This implementation is similar to _BroadcastAIList_, but computing is moved as-is from the driver to executors.
1. Bucketize both _database_ and _query_ datasets.
2. Group the _query_ data by key.
3. Map each group and compute AIList on each partition separately.
4. Cogroup _query_ with _database_.
5. Perform the join in parallel on executors.
6. Deduplicate the results.

The algorithm does not load results back to the driver, but its efficiency takes heavy hits from GC pauses.

### PartitionedNativeAIList
This implementation is similar to _NativeAIList_, but it also utilises _Bucketizer_ to scale down partitions' sizes.
    The only differences are using more fine-grained partitioning and deduplicating the results. However, it yields 
    better results and offers quite similar performance to the Broadcast algorithm while being horizontally scalable - 
    that is until caching stops working in the iterative phase. 

### SparkNative (to be renamed: PartitionedSparkNative)
An alternative join implementation without using any specific algorithm: 
1. Bucketize both datasets.
2. Perform full join.
3. Filter out relevant join.

## Benchmarking

### Running on Java 17
Running on newer Java versions requires adding exports' parameters as a JVM option:
```bash
JAVA_OPTS=""
JAVA_OPTS="$JAVA_OPTS --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-exports java.base/sun.security.action=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens java.base/java.lang=ALL-UNNAMED"
```

### Execution times 
Please see [the Basic benchmark](jupyter-lab/Basic%20benchmark.ipynb) jupyter notebook for execution times charts.

## External links
- [Original AIList article](https://academic.oup.com/bioinformatics/article/35/23/4907/5509521)
- [Original AIList implementation on GitHub](https://github.com/databio/AIList/)
- [IITII implementation on Apache Spark - 1](https://github.com/Wychowany/mgr-iitii/tree/main)
- [IITII implementation on Apache Spark - 2](https://github.com/Wychowany/mgr-code/tree/main)