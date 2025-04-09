

## Join algorithm
This library uses Augmented me.kosik.interwalled.ailist.Interval List 
(me.kosik.interwalled.ailist.AIList) data structure to store and effectively 
join intervals in SQL join. The relevant paper is published
[on biorxiv.org](https://www.biorxiv.org/content/10.1101/593657v1).

## Implementation details



### Running on Java 17
Running on newer Java versions requires adding `--add-exports java.base/sun.nio.ch=ALL-UNNAMED` 
    as a JVM option. 

## Rough benchmark
Execution measured for 10.000 buckets, Spark in _local[16]_ mode, 32G memory for the driver.

### Execution times 


| Algorithm                     | Dataset          | Execution time | Error message (if failed)                                                                                        |
|-------------------------------|------------------|----------------|------------------------------------------------------------------------------------------------------------------|
| BroadcastAIListIntervalJoin   | Mirror join, 80M | N/A            | Total size of serialized results of 15 tasks (1083.2 MiB) is bigger than spark.driver.maxResultSize (1024.0 MiB) |
| PartitionedAIListIntervalJoin | Mirror join, 80M | 78 807 ms      |                                                                                                                  |
| SparkNativeIntervalJoin       | Mirror join, 80M | 442 937 ms     |                                                                                                                  |




## External links
- [Original AIList article](https://academic.oup.com/bioinformatics/article/35/23/4907/5509521)
- [Original AIList implementation on GitHub](https://github.com/databio/me.kosik.interwalled.ailist.AIList/)
- [IITII implementation on Apache Spark - 1](https://github.com/Wychowany/mgr-iitii/tree/main)
- [IITII implementation on Apache Spark - 2](https://github.com/Wychowany/mgr-code/tree/main)