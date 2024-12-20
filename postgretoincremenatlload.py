from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("IncrementalLoadWithChanges") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 2: Load existing data from Hive
hive_table = "bigdata_nov_2024.hasan_vechile_sales"
try:
    hive_df = spark.sql(f"SELECT * FROM {hive_table}")
    print("Existing data loaded from Hive.")
except Exception as e:
    hive_df = None
    print(f"Hive table not found or empty: {e}. Proceeding with a full load.")

# Step 3: Load full data from PostgreSQL
df_postgres = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "vechile_sales") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .load()

# Step 4: Identify incremental changes
if hive_df:
    # Perform a join to find rows with changes
    incremental_df = df_postgres.alias("src").join(
        hive_df.alias("tgt"),
        on=["id"],  # Assuming 'id' is the unique key
        how="leftanti"  # Select rows that do not match
    ).select("src.*")  # Fetch the source rows with changes
else:
    # No existing data, load everything
    incremental_df = df_postgres

# Step 5: Write the changes to Hive
if not incremental_df.rdd.isEmpty():
    incremental_df.write.mode("overwrite").saveAsTable(hive_table)
    print("Incremental data successfully loaded to Hive.")
else:
    print("No changes detected. Hive table is up-to-date.")
