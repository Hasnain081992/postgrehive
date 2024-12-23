import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, lower, current_date

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("FullLoadWithTransformation").enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "person").option("user", "consultants").option("password", "WelcomeItc@2022").load()
df.printSchema()

    # Step 2: Data Transformation - Generate email address
df =(df.withColumn("email", concat(lower(col("firstname")), lit("."), lower(col("lastname")), lit("@example.com"))).withColumn("ingestion_date", current_date()))


print("Transformed Schema:")
df.printSchema()

    # Step 3: Write data to Hive
df.write.mode("overwrite").saveAsTable(hive_table)
print("Full load with transformation completed successfully.")
except Exception as e:
print(f"Error during full load: {e}")

finally:
spark.stop()
