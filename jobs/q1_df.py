from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, hour, when, avg, sum, count, countDistinct

def main():
    spark = SparkSession.builder.appName("Q1_Demand_Profile_DF").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Η διαδρομή των καθαρών δεδομένων σου
    input_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"

    # 1. Φόρτωση Δεδομένων
    df = spark.read.parquet(input_path)

    # 2. Εφαρμογή Φίλτρων Ποιότητας & Εξατομίκευσης (ΑΜ: 2121099 -> Ώρες: 3,4,5,6 | Ημέρες: 20,21,22)
    filtered_df = df.filter(
        col("pickup_ts").isNotNull() & col("dropoff_ts").isNotNull() &
        (col("duration_minutes") > 0) & 
        (col("trip_distance") > 0) & 
        (col("total_amount") > 0) &
        (dayofmonth(col("pickup_ts")).isin(20, 21, 22)) &
        (hour(col("pickup_ts")).isin(3, 4, 5, 6))
    )

    # 3. Δημιουργία Βοηθητικών Στηλών (time_band)
    processed_df = filtered_df.withColumn("pickup_hour", hour(col("pickup_ts"))) \
        .withColumn("pickup_date", col("pickup_ts").cast("date")) \
        .withColumn("time_band", 
            when((col("pickup_hour") >= 0) & (col("pickup_hour") <= 5), "Night")
            .when((col("pickup_hour") >= 6) & (col("pickup_hour") <= 11), "Morning")
            .when((col("pickup_hour") >= 12) & (col("pickup_hour") <= 16), "Afternoon")
            .when((col("pickup_hour") >= 17) & (col("pickup_hour") <= 21), "Evening")
            .otherwise("Late")
        )

    # 4. Υπολογισμός Συνολικών Trips στο Προσωπικό Παράθυρο (απαιτείται για το trip_share)
    total_personal_trips = processed_df.count()

    # 5. Ομαδοποίηση και Υπολογισμός Μετρικών (Απαίτηση Q1)
    metrics_df = processed_df.groupBy("pickup_date", "pickup_hour").agg(
        count("*").alias("trips"),
        countDistinct("pu_location_id").alias("unique_pickup_zones"),
        avg("passenger_count").alias("avg_passenger_count"),
        avg("duration_minutes").alias("avg_duration_minutes"),
        avg("trip_distance").alias("avg_trip_distance"),
        avg("total_amount").alias("avg_total_amount"),
        sum("total_amount").alias("total_revenue")
    ).withColumn(
        "trip_share_in_personal_window", (col("trips") / total_personal_trips * 100)
    )

    # 6. Ταξινόμηση και Εμφάνιση Top-K (K = 12)
    final_result = metrics_df.orderBy(
        col("trips").desc(), 
        col("total_revenue").desc(), 
        col("pickup_date").asc(), 
        col("pickup_hour").asc()
    ).limit(12)

    print("\n--- Top 12 Ώρες/Ημέρες Ζήτησης (DataFrame API) ---")
    final_result.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
