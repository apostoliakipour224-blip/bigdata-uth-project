import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, round, radians, sin, cos, atan2, sqrt, pow, mean

def main():
    # 1. Αρχικοποίηση του Spark
    spark = SparkSession.builder.appName("Q2_DataFrame_Builtins").getOrCreate()

    # Φόρτωση του Parquet
        df = spark.read.parquet(data_path)
        
        # ΠΡΟΣΘΕΣΕ ΑΥΤΟ:
        print("ΟΙ ΣΤΗΛΕΣ ΠΟΥ ΒΡΗΚΑ ΕΙΝΑΙ:")
        print(df.columns) 
        
    
    # 2. Το ΣΩΣΤΟ μονοπάτι για τα δεδομένα του 2024
    data_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    
    start_time = time.time()
    print("\n" + "="*50)
    print("=== ΞΕΚΙΝΑΕΙ Η ΦΟΡΤΩΣΗ ΤΩΝ ΔΕΔΟΜΕΝΩΝ ΤΟΥ 2015 ===")
    
    try:
        # Φόρτωση του Parquet
        df = spark.read.parquet(data_path)
        print("Τα δεδομένα φορτώθηκαν με επιτυχία!")
        
        # 1. Φιλτράρισμα Έγκυρων Εγγραφών (Καθαρισμός)
        df_clean = df.filter(
            (col("trip_distance") > 0) & 
            (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
            (col("pickup_longitude").between(-75, -73)) & 
            (col("pickup_latitude").between(40, 42)) &
            (col("dropoff_longitude").between(-75, -73)) & 
            (col("dropoff_latitude").between(40, 42))
        )

        # 2. Υπολογισμός Διάρκειας (ώρες) και Ταχύτητας (km/h)
        df_metrics = df_clean.withColumn(
            "duration_hours", 
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600.0
        ).withColumn(
            "speed_kmh", 
            (col("trip_distance") * 1.60934) / col("duration_hours")
        )

        # 3. Υπολογισμός Haversine (μόνο με built-in functions)
        R = 6371.0 # Ακτίνα της Γης σε χλμ
        lat1 = radians(col("pickup_latitude"))
        lat2 = radians(col("dropoff_latitude"))
        lon1 = radians(col("pickup_longitude"))
        lon2 = radians(col("dropoff_longitude"))

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = pow(sin(dlat / 2), 2) + cos(lat1) * cos(lat2) * pow(sin(dlon / 2), 2)
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        df_metrics = df_metrics.withColumn("haversine_dist_km", R * c)

        # 4. Υπολογισμός Detour Ratio & Φιλτράρισμα Ακραίων Τιμών (Outliers)
        df_metrics = df_metrics.filter(col("haversine_dist_km") > 0) \
            .withColumn("detour_ratio", (col("trip_distance") * 1.60934) / col("haversine_dist_km")) \
            .filter((col("speed_kmh") > 0) & (col("speed_kmh") < 120))

        # === ΔΗΜΙΟΥΡΓΙΑ ΤΩΝ 3 ΠΙΝΑΚΩΝ ===
        # Πίνακας 1: Σύνοψη ανά ώρα
        summary_by_hour = df_metrics.withColumn("hour", hour("tpep_pickup_datetime")) \
            .groupBy("hour") \
            .agg(
                round(mean("speed_kmh"), 2).alias("avg_speed_kmh"),
                round(mean("detour_ratio"), 2).alias("avg_detour_ratio")
            ).orderBy("hour")

        # Πίνακας 2: Ταχύτερες διαδρομές (Top 5)
        fastest_trips = df_metrics.select(
            "tpep_pickup_datetime", "tpep_dropoff_datetime", 
            round("trip_distance", 2).alias("dist_miles"), 
            round("speed_kmh", 2).alias("speed_kmh")
        ).orderBy(col("speed_kmh").desc()).limit(5)

        # Πίνακας 3: Πιο αργές διαδρομές / Πιθανή Συμφόρηση (Top 5)
        slowest_trips = df_metrics.select(
            "tpep_pickup_datetime", "tpep_dropoff_datetime", 
            round("trip_distance", 2).alias("dist_miles"), 
            round("speed_kmh", 2).alias("speed_kmh")
        ).orderBy(col("speed_kmh").asc()).limit(5)

        # === ΕΚΤΥΠΩΣΗ ΑΠΟΤΕΛΕΣΜΑΤΩΝ ===
        print("\n=== 1. ΜΕΣΗ ΤΑΧΥΤΗΤΑ & DETOUR RATIO ΑΝΑ ΩΡΑ ===")
        summary_by_hour.show(24)

        print("\n=== 2. ΟΙ 5 ΤΑΧΥΤΕΡΕΣ ΔΙΑΔΡΟΜΕΣ ===")
        fastest_trips.show()

        print("\n=== 3. ΟΙ 5 ΠΙΟ ΑΡΓΕΣ ΔΙΑΔΡΟΜΕΣ (ΣΥΜΦΟΡΗΣΗ) ===")
        slowest_trips.show()

    except Exception as e:
        print("ΣΦΑΛΜΑ:", str(e))
        
    end_time = time.time()
    print("="*50)
    print(f"Συνολικός χρόνος εκτέλεσης: {end_time - start_time} δευτερόλεπτα")
    print("="*50 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()