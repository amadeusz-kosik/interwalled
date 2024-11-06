

## Join algorithm
This library uses Augmented me.kosik.interwalled.ailist.Interval List (me.kosik.interwalled.ailist.AIList) data structure to store and effectively 
join intervals in SQL join. The relevant paper is published
[on biorxiv.org](https://www.biorxiv.org/content/10.1101/593657v1) and the reference 
implementation can be found [on GitHub](https://github.com/databio/me.kosik.interwalled.ailist.AIList/). 

## Implementation details

### Running on Java 17
Running on newer Java versions requires adding `--add-exports java.base/sun.nio.ch=ALL-UNNAMED` 
    as a JVM option. 

## External links
- [Original me.kosik.interwalled.ailist.AIList article](https://academic.oup.com/bioinformatics/article/35/23/4907/5509521)
- [Original me.kosik.interwalled.ailist.AIList implementation on GitHub](https://github.com/databio/me.kosik.interwalled.ailist.AIList/)
- [IITII implementation on Apache Spark - 1](https://github.com/Wychowany/mgr-iitii/tree/main)
- [IITII implementation on Apache Spark - 2](https://github.com/Wychowany/mgr-code/tree/main)