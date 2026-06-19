import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, year, hour, dayofmonth, to_date

def main():
    spark = SparkSession.builder.appName("Prepare_Parquet_2024").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Ορισμός Ρητού Σχήματος για το 2024 (Απαίτηση Εργασίας)
    schema_2024 = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True)
    ])

    input_csv = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2024.csv"
    
    # Το output_path παίρνει το HADOOP_USER_NAME αυτόματα από το περιβάλλον σου
    import os
    username = os.environ.get("HADOOP_USER_NAME", "root")
    output_parquet = f"hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2015"

    start_time = time.time()

    # 2. Φόρτωση CSV με το ρητό σχήμα
    df_raw = spark.read.csv(input_csv, header=True, schema=schema_2024)
    raw_count = df_raw.count()

    # 3. Μετατροπή Ημερομηνιών και Δημιουργία Βοηθητικών Στηλών
    df_transformed = df_raw \
        .withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
        .withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
        .withColumnRenamed("VendorID", "vendor_id") \
        .withColumnRenamed("PULocationID", "pu_location_id") \
        .withColumnRenamed("DOLocationID", "do_location_id") \
        .withColumn("pickup_date", to_date(col("pickup_ts"))) \
        .withColumn("pickup_day", dayofmonth(col("pickup_ts"))) \
        .withColumn("pickup_hour", hour(col("pickup_ts"))) \
        .withColumn("duration_minutes", (col("dropoff_ts").cast("long") - col("pickup_ts").cast("long")) / 60.0) \
        .withColumn("trip_distance_km", col("trip_distance") * 1.60934)

    # 4. Φιλτράρισμα: Πετάμε τα nulls και κρατάμε ΜΟΝΟ το έτος 2024
    df_filtered = df_transformed.filter(col("pickup_ts").isNotNull() & col("dropoff_ts").isNotNull())
    valid_dates_df = df_filtered.filter(year(col("pickup_ts")) == 2024)
    
    final_count = valid_dates_df.count()

    # 5. Αποθήκευση σε Parquet με διαμερισμό (partitioning) ανά ημέρα (pickup_day)
    valid_dates_df.write.mode("overwrite").partitionBy("pickup_day").parquet(output_parquet)

    end_time = time.time()

    # 6. Δημιουργία των Μετρικών (Απαίτηση Εργασίας)
    metrics = {
        "dataset": "yellow_tripdata_2024",
        "raw_rows": raw_count,
        "processed_rows": final_count,
        "dropped_rows": raw_count - final_count,
        "conversion_time_seconds": round(end_time - start_time, 2),
        "partition_column": "pickup_day",
        "output_path": output_parquet
    }

    print("\n--- Μετρικές Προετοιμασίας ---")
    print(json.dumps(metrics, indent=4))

    spark.stop()

if __name__ == "__main__":
    main()
