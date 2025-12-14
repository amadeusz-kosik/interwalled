# Interval join helper for Apache Spark
[![CI](https://github.com/amadeusz-kosik/interwalled/actions/workflows/unit_tests.yml/badge.svg)](https://github.com/amadeusz-kosik/interwalled/actions/workflows/unit_tests.yml)
[![Qodana](https://github.com/amadeusz-kosik/interwalled/actions/workflows/code_quality.yml/badge.svg)](https://github.com/amadeusz-kosik/interwalled/actions/workflows/code_quality.yml)

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

## Data preprocessing
The goal of preprocessing is to call Spark's _.cogroup_ function on smaller partitions 
    (using _key_ and _bucket_ instead of only the former one). Call to _.cogroup_ function is necessary 
    to perform the join and is part of the actual join algorithm. If the preprocessing increases the number
    of rows by duplicating them, then after handling the join the algorithm must also call _.distinct_ 
    to get rid of those duplicates (in above example: A2-B1 pair would appear two times).

### Bucketing
The main goal of the bucketizer is to roughly split all data into separate buckets, trying to limit range 
    of possible pairs from whole dataset to a single bucket. It does so by dividing the domain 
    (range of [min(from) ... max(to)] values) into buckets ([a ... b), [b ... c)) and assigning each row 
    to all buckets it covers. Perfect bucketing assigns each row to a single bucket and produces buckets small 
    enough to optimize the join time.

Bucketing algorithm is defined below; min and max parameters are respectively min(from) and max(to) of all rows 
    in the bigger dataset). Scale parameter is computed from perRows parameter, defined by user: 
    `math.max(1L, datasetCount / perRows)`.

```scala
private lazy val bucketize: UserDefinedFunction = {
  F.udf((from: Long, to: Long, min: Long, max: Long, scale: Long) => {
    val normalizedFrom = (from - min) * scale / (max - min)
    val normalizedTo   = ( to  - min) * scale / (max - min)
    
    (normalizedFrom to normalizedTo).toArray
  })
}
```
In case of massive overlapping in the input datasets, bucketing will result with data multiplication and possible 
    degradation of the performance; non-massive scenario may experience better scalability.

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

### Salting
Salting is a technique meant for more granular distribution of the heavier dataset: one of the datasets 
    is randomly split into N groups, whereas each row of the other one is multiplied N times. This 
    implementation does split both datasets in order to create smaller partitions (although with larger 
    overall number of rows):

```scala
val lhsSalted = input.lhsData
  .withColumn("_bucket_y", F.floor(F.rand() * F.lit(scale)).cast(DataTypes.LongType))
  .withColumn("_bucket_x", F.explode(F.lit((0L until scale).toArray)))
  .withColumn(BUCKET, F.concat_ws(":", F.col(BUCKET), F.col("_bucket_x") * F.lit(scale) + F.col("_bucket_y")))
  .drop("_bucket_x", "_bucket_y")
  .as[BucketedInterval]

val rhsSalted = input.rhsData
  .withColumn("_bucket_x", F.floor(F.rand() * F.lit(scale)).cast(DataTypes.LongType))
  .withColumn("_bucket_y", F.explode(F.lit((0L until scale).toArray)))
  .withColumn(BUCKET, F.concat_ws(":", F.col(BUCKET), F.col("_bucket_x") * F.lit(scale) + F.col("_bucket_y")))
  .drop("_bucket_x", "_bucket_y")
  .as[BucketedInterval]
```

## Join implementations
This library uses algorithms and data structures based on Augmented Interval List (AIList) data structure to store
and effectively join intervals in SQL join. The relevant paper is published
[on biorxiv.org](https://www.biorxiv.org/content/10.1101/593657v1).

### Spark native approach - SparkNative
This is a reference implementation utilizing only Spark's built-in optimizations. It is implemented 
    as full cartesian join followed by filtering operation:
```sql
lhs CROSS JOIN rhs
WHERE lhs.start >= rhs.end AND rhs.start >= lhs.end
```

### Driver-based optimization - BroadcastAIList
This is an Augumented Interval List implementation, where the AIList is computed on the driver node, then 
    broadcasted to executors and joined with data there:
```scala
val lhs = lhsDF.collect()
val lhsAIList = spark.broadcast(new AIList(lhs))
val joinedData = rhs.mapPartitions(rhsPartition => join(lhsAIList, rhsPartition)
```

The heavy downside of this approach is limitation of the smaller dataset's size - since AIList is computed 
    on a single node, it does not scale out well. Moreover, Spark's documentation does not encourage performing 
    heavy computation on the driver itself.

### DataFrame cached implementation - CachedNativeAIList
The DataFrame implementations rely on Spark's native DataFrame operations to compute the AIList. It uses 
    windowing operations to perform AIList items' extraction and iteratively build consecutive components:

```scala
val sourceInputLookaheadWindow = Window
  .partitionBy(KEY, BUCKET)
  .orderBy(FROM, TO)
  .rowsBetween(1, intervalsCountToCheckLookahead)

val preparedDF = sourceDF
  .withColumn("_ailist_lookahead",                    F.collect_list(TO).over(sourceInputLookaheadWindow))
  .withColumn("_ailist_lookahead_overlapping",        F.filter(F.col("_ailist_lookahead"), _ <= F.col(TO)))
  .withColumn("_ailist_lookahead_overlapping_count",  F.size(F.col("_ailist_lookahead_overlapping")))
  .withColumn("_ailist_lookahead_overlapping_keeper", F.col("_ailist_lookahead_overlapping_count") < F.lit(intervalsCountToTriggerExtraction))

val extractedDF  = preparedDF
  .filter(F.col("_ailist_lookahead_overlapping_keeper") === true)
  .withColumn(_COMPONENT, F.lit(iteration))
```
Since the whole algorithm is iterative by nature, this implementation relies on caching to speed up the computation.

### DataFrame storage-backed implementation - CheckpointedNativeAIList
This algorithm is a modified version of cached-native-ailist, where using caching mechanism to optimize execution 
    time is replaced with use of temporary HDFS / local / table to append consecutive components. This limits 
    number of recomputations of already processed rows.

### RDD low-level implementation - RDDAIList
Last implementation uses similar approach to driver-ailist, but each AILists are computed in parallel 
    on executors. It is not speeded up by Spark's optimizations, but still should allow for better scaling.

## Benchmarking
In order to verify scalability of the application in different environments, benchmarking was run on 
    a different Apache Spark clusters:
- spark-worker-m: worker with 4G memory, 4 cores
- spark-worker-l: worker with 10G memory, 10 cores

All clusters were using 4G driver in order to check scalability via changing workers' configuration, 
    not driver's one.

### Running on Java 17
Running on newer Java versions requires adding exports' parameters as a JVM option:
```bash
JAVA_OPTS=""
JAVA_OPTS="$JAVA_OPTS --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-exports java.base/sun.security.action=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens java.base/java.lang=ALL-UNNAMED"
```

### Execution times 
Please see [the Basic benchmark](notebooks/main.ipynb) jupyter notebook for execution times charts.

## External links
- [Original AIList article](https://academic.oup.com/bioinformatics/article/35/23/4907/5509521)
- [Original AIList implementation on GitHub](https://github.com/databio/AIList/)
- [IITII implementation on Apache Spark - 1](https://github.com/Wychowany/mgr-iitii/tree/main)
- [IITII implementation on Apache Spark - 2](https://github.com/Wychowany/mgr-code/tree/main)