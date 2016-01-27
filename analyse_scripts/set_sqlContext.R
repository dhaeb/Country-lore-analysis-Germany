Sys.setenv(SPARK_HOME="/usr/local/spark-1.5.1-bin-hadoop2.6")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"),"R","lib"), .libPaths()))
# Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.3.0" "sparkr-shell"')
# Sys.setenv('SPARKR_SUBMIT_ARGS'='"--jars" "/usr/lib/spark-1.5.1-bin-hadoop2.6/lib/spark-csv_2.11-1.2.0.jar,/usr/lib/spark-1.5.1-bin-hadoop2.6/lib/commons-csv-1.2.jar" "sparkr-shell"')

# https://github.com/databricks/spark-csv
library(SparkR)
# sc <- sparkR.init(master="local")
#  if creating context through init you can specify the packages with the packages argument
sc <- sparkR.init(sparkPackages="com.databricks:spark-csv_2.10:1.3.0")
sqlContext <- sparkRSQL.init(sc)
