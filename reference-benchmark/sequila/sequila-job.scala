import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SequilaSession

import org.apache.spark.sql.{functions => F}

val path = "/Users/iw-user/Git/FEIT/Interwalled/interwalled-data-generator/data/maximum-size-of-input"

val spark      = SparkSession.builder().appName("Interval benchmark").getOrCreate()
val sqlsession = SequilaSession(spark)

def parse(input: DataFrame): DataFrame =
  input
    .withColumn("text", F.split(F.col("value"), "\\s+"))
    .select(
      F.col("text").getItem(0).alias("contig"),
      F.col("text").getItem(1).alias("pos_start"),
      F.col("text").getItem(2).alias("pos_end"),
    )

val database   = sqlsession.read
  .text(path + "/input-80M.bed")
  .transform(parse)
  .alias("database")

val query      = sqlsession.read
  .text(path + "/input-80M.bed")
  .transform(parse)
  .alias("query")


database
  .join(query,
    (database.col("pos_start") <= query.col("pos_end")) &&
    (database.col("pos_end") >= query.col("pos_start")) &&
    (database.col("contig") === query.col("contig"))
  )
  .select(
    database.col("pos_start").alias("lhs_start"),
    database.col("pos_end").alias("lhs_end"),
    query.col("pos_start").alias("rhs_start"),
    query.col("pos_end").alias("rhs_end"),
    database.col("contig")
  )
  .foreach (_ => ())

scala.io.StdIn.readLine()