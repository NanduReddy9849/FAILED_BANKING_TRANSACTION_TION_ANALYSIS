from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CleanMergeTransactions").getOrCreate()

# Load all CSV files
df = spark.read.option("header", "true").option("inferSchema", "true").csv("gs://narayana123/failed_transaction_data/*.csv")

# Remove rows with all columns null
df_clean = df.dropna(how='all')

# Remove blank strings in any column
for column in df_clean.columns:
    df_clean = df_clean.filter((col(column).isNotNull()) & (col(column) != ''))
failed_df = df.filter(df["status"] == "FAILED")
# MySQL connection properties 
jdbc_url = "jdbc:mysql://34.132.106.6:3306/p1project-database"
properties = { "user": "mysql-instance",
"password": "12345", 
"driver": "com.mysql.cj.jdbc.Driver" } 
failed_df.write.jdbc(url=jdbc_url, table="failed_transactions", mode="overwrite", properties=properties) 
spark.stop()

