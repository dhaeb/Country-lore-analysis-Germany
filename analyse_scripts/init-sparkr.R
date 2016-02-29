library(SparkR)
sc <- sparkR.init(sparkPackages = "com.databricks:spark-csv_2.10:1.2.0")
sqlContext <- sparkRSQL.init(sc)
