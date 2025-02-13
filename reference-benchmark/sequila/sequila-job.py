import os

from pysequila import SequilaSession

ss = (SequilaSession
      .builder
      .config("spark.jars.packages", "org.biodatageeks:sequila_2.12:1.1.0")
      .config("spark.driver.memory", "4g")
      .getOrCreate()
      )

database_path = os.getenv("SQ_DATA_PATH_DB")
query_path = os.getenv("SQ_DATA_PATH_Q")
out_path = os.getenv("SQL_DATA_OUT")

database = ss.read.format("org.biodatageeks.sequila.datasources.BED.BEDDataSource").load(database_path)
query = ss.read.format("org.biodatageeks.sequila.datasources.BED.BEDDataSource").load(query_path)

overlap_cond = (database.pos_end >= query.pos_start) & (database.pos_start <= query.pos_end)
result = (database.join(query, (database.contig == query.contig) & overlap_cond))
result.write.csv("out_path", mode="append")

input("Press Enter to continue...")
