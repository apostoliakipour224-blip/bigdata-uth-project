import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, sum, avg, count, round, coalesce, lit

def main():
    spark = SparkSession.builder.appName("Q3_DataFrame_CSV").getOrCreate()
    
    # Μονοπάτια στα αρχεία CSV του κοινόχρηστου φακέλου
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2024.csv"
    zone_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/taxi_zone_lookup.csv"
    
    start_time = time.time()
    print("\n" + "="*50)
    print("=== ENΑΡΞΗ ΥΠΟΛΟΓΙΣΜΟΥ Q3 (DATAFRAME API - CSV) ===")
    
    try:
        # 1. Φόρτωση Δεδομένων
        print("Φόρτωση αρχείων CSV...")
        df_trips = spark.read.csv(trips_path, header=True, inferSchema=True)
        df_zones = spark.read.csv(zone_path, header=True, inferSchema=True)
        
        # 2. Υπολογισμός διάρκειας σε λεπτά και φιλτράρισμα έγκυρων εγγραφών
        df_trips = df_trips.withColumn(
            "duration_minutes", 
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0
        )
        
        df_filtered = df_trips.filter(
            (col("duration_minutes") > 0) & 
            (col("trip_distance") > 0) & 
            (col("fare_amount") > 0) & 
            (col("total_amount") > 0)
        )
        
        # 3. Δημιουργία Distance Buckets (Κατηγορίες απόστασης)
        df_bucketed = df_filtered.withColumn(
            "distance_bucket",
            when(col("trip_distance") < 1, "very_short")
            .when((col("trip_distance") >= 1) & (col("trip_distance") < 3), "short")
            .when((col("trip_distance") >= 3) & (col("trip_distance") < 10), "medium")
            .otherwise("long")
        )
        
        # 4. Πρώτο Join με taxi_zone_lookup για την τοποθεσία επιβίβασης (PULocationID)
        df_joined = df_bucketed.join(
            df_zones.withColumnRenamed("Borough", "pickup_borough").withColumnRenamed("Zone", "pickup_zone"),
            df_bucketed.PULocationID == df_zones.LocationID,
            "inner"
        )
        
        # 5. Διαχείριση Surcharges (αντικατάσταση null/ανύπαρκτων στηλών με 0)
        # Ελέγχουμε και προσθέτουμε τις στήλες με ασφάλεια χρησιμοποιώντας coalesce
        available_columns = df_joined.columns
        surcharge_cols = ["extra", "mta_tax", "tolls_amount", "improvement_surcharge", "congestion_surcharge", "airport_fee"]
        
        surcharge_expr = lit(0)
        for c in surcharge_cols:
            if c in available_columns:
                surcharge_expr = surcharge_expr + coalesce(col(c), lit(0))
        
        df_with_surcharge = df_joined.withColumn("total_surcharges", surcharge_expr)
        
        # 6. Ομαδοποίηση και Υπολογισμός Μετρικών
        print("Υπολογισμός συγκεντρωτικών μετρικών ανά ζώνη και κατηγορία απόστασης...")
        df_aggregated = df_with_surcharge.groupBy("pickup_borough", "pickup_zone", "distance_bucket") \
            .agg(
                count("total_amount").alias("trips"),
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_revenue_per_trip"),
                (sum("total_amount") / sum("trip_distance")).alias("revenue_per_mile"),
                (sum("total_amount") / sum("duration_minutes")).alias("revenue_per_minute"),
                avg("fare_amount").alias("avg_fare_amount"),
                avg("tip_amount").alias("avg_tip_amount"),
                (sum("total_surcharges") / sum("total_amount")).alias("surcharge_share")
            )
            
        # Κρατάμε μια έκδοση ΠΡΙΝ το φίλτρο υποστήριξης για την ερώτηση (β) της ανάλυσης
        df_aggregated.cache()
        
        # Ορισμός ορίου υποστήριξης (trips >= 50)
        MIN_TRIPS = 50
        df_supported = df_aggregated.filter(col("trips") >= MIN_TRIPS)
        
        # Ορίζουμε το K για τις κορυφαίες ζώνες
        K = 5
        
        # 7. Εξαγωγή Αποτελεσμάτων
        print(f"\n=== TOP {K} ΖΩΝΕΣ ΒΑΣΕΙ TOTAL REVENUE (ΧΩΡΙΣ ΟΡΙΟ) ===")
        df_aggregated.orderBy(col("total_revenue").desc()).select("pickup_borough", "pickup_zone", "distance_bucket", "trips", round("total_revenue", 2).alias("total_revenue")).show(K)
        
        print(f"\n=== TOP {K} ΖΩΝΕΣ ΒΑΣΕΙ REVENUE PER MILE (ΜΕ ΟΡΙΟ TRIPS >= {MIN_TRIPS}) ===")
        df_supported.orderBy(col("revenue_per_mile").desc()).select("pickup_borough", "pickup_zone", "distance_bucket", "trips", round("revenue_per_mile", 2).alias("rev_per_mile")).show(K)
        
        print(f"\n=== TOP {K} ΖΩΝΕΣ ΒΑΣΕΙ REVENUE PER MINUTE (ΜΕ ΟΡΙΟ TRIPS >= {MIN_TRIPS}) ===")
        df_supported.orderBy(col("revenue_per_minute").desc()).select("pickup_borough", "pickup_zone", "distance_bucket", "trips", round("revenue_per_minute", 2).alias("rev_per_min")).show(K)
        
        # Παράδειγμα για την ερώτηση (β): Ζώνη που φαίνεται αποδοτική αλλά έχει ελάχιστα trips
        print(f"\n=== ΠΑΡΑΔΕΙΓΜΑ ΑΚΡΑΙΩΝ ΤΙΜΩΝ (OUTLIERS) ΧΩΡΙΣ ΤΟ ΟΡΙΟ ΥΠΟΣΤΗΡΙΞΗΣ ===")
        df_aggregated.filter(col("trips") < 10).orderBy(col("revenue_per_mile").desc()).select("pickup_borough", "pickup_zone", "distance_bucket", "trips", round("revenue_per_mile", 2).alias("rev_per_mile")).show(3)

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
        
    end_time = time.time()
    print("\n" + "="*50)
    print(f"Συνολικός χρόνος εκτέλεσης (CSV): {end_time - start_time} δευτερόλεπτα")
    print("="*50 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()