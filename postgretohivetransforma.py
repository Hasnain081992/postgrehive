from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, lower, current_date

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("FullLoadWithTransformation").enableHiveSupport().getOrCreate()

# JDBC Configuration
jdbc_url = "jdbc:postgresql://18.132.73.146:5432/testdb"
jdbc_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}
table_name = "person"
hive_table = "bigdata_nov_2024.person_full_load"
#done
try:
    # Step 1: Load data from PostgreSQL
    df = spark.read.format("jdbc").options(
        url=jdbc_url,
        dbtable=table_name,
        **jdbc_properties
    ).load()

    print("Original Schema:")
    df.printSchema()

    # Step 2: Data Transformation - Generate email address
    transformed_df = (
        df
        # Ensure first and last names are lowercase for the email address
        .withColumn("email", concat(lower(col("firstname")), lit("."), lower(col("lastname")), lit("@example.com")))
        
        # Optional: Add an ingestion date column
        .withColumn("ingestion_date", current_date())
    )

    print("Transformed Schema:")
    transformed_df.printSchema()

    # Step 3: Write data to Hive
    transformed_df.write.mode("overwrite").saveAsTable(hive_table)
    print("Full load with transformation completed successfully.")

except Exception as e:
    print(f"Error during full load: {e}")

finally:
    spark.stop()
