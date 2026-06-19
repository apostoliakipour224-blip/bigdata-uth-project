import time
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Q2_Spark_SQL").getOrCreate()
    
    # Το γνωστό path για τα δεδομένα
    data_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2015.csv"
    
    start_time = time.time()
    print("\n" + "="*50)
    print("=== ΞΕΚΙΝΑΕΙ Η ΦΟΡΤΩΣΗ ΤΩΝ ΔΕΔΟΜΕΝΩΝ ΤΟΥ 2015 (SQL) ===")
    
    try:
        # Φόρτωση δεδομένων
        df = spark.read.csv(data_path, header=True, inferSchema=True)
        
        # Καταχώρηση του DataFrame ως προσωρινό SQL table
        df.createOrReplaceTempView("trips")
        
        # ==========================================
        # ΤΟ ΜΕΓΑΛΟ SQL ΕΡΩΤΗΜΑ (ΕΝΣΩΜΑΤΩΝΕΙ ΤΑ ΠΑΝΤΑ)
        # ==========================================
        sql_query = """
        WITH CleanedTrips AS (
            SELECT *,
                   (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600.0 AS duration_hours
            FROM trips
            WHERE trip_distance > 0 
              AND tpep_dropoff_datetime > tpep_pickup_datetime
              AND pickup_longitude BETWEEN -75 AND -73
              AND pickup_latitude BETWEEN 40 AND 42
              AND dropoff_longitude BETWEEN -75 AND -73
              AND dropoff_latitude BETWEEN 40 AND 42
        ),
        CalculatedMetrics AS (
            SELECT *,
                   (trip_distance * 1.60934) / duration_hours AS speed_kmh,
                   -- Μαθηματικός τύπος Haversine απευθείας σε SQL
                   6371.0 * 2 * atan2(
                       sqrt(
                           pow(sin(radians(dropoff_latitude - pickup_latitude) / 2), 2) + 
                           cos(radians(pickup_latitude)) * cos(radians(dropoff_latitude)) * pow(sin(radians(dropoff_longitude - pickup_longitude) / 2), 2)
                       ), 
                       sqrt(1 - (
                           pow(sin(radians(dropoff_latitude - pickup_latitude) / 2), 2) + 
                           cos(radians(pickup_latitude)) * cos(radians(dropoff_latitude)) * pow(sin(radians(dropoff_longitude - pickup_longitude) / 2), 2)
                       ))
                   ) AS haversine_dist_km
            FROM CleanedTrips
        ),
        FinalMetrics AS (
            SELECT *,
                   (trip_distance * 1.60934) / haversine_dist_km AS detour_ratio,
                   hour(tpep_pickup_datetime) AS hour_of_day
            FROM CalculatedMetrics
            WHERE haversine_dist_km > 0
              AND ((trip_distance * 1.60934) / duration_hours) > 0 
              AND ((trip_distance * 1.60934) / duration_hours) < 120
        )
        
        """
        
        # Πίνακας 1
        summary_by_hour = spark.sql(sql_query + """
            SELECT hour_of_day AS hour, 
                   ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh, 
                   ROUND(AVG(detour_ratio), 2) AS avg_detour_ratio
            FROM FinalMetrics
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """)
        
        # Πίνακας 2
        fastest_trips = spark.sql(sql_query + """
            SELECT tpep_pickup_datetime, tpep_dropoff_datetime, 
                   ROUND(trip_distance, 2) AS dist_miles, 
                   ROUND(speed_kmh, 2) AS speed_kmh
            FROM FinalMetrics
            ORDER BY speed_kmh DESC
            LIMIT 5
        """)
        
        # Πίνακας 3
        slowest_trips = spark.sql(sql_query + """
            SELECT tpep_pickup_datetime, tpep_dropoff_datetime, 
                   ROUND(trip_distance, 2) AS dist_miles, 
                   ROUND(speed_kmh, 2) AS speed_kmh
            FROM FinalMetrics
            ORDER BY speed_kmh ASC
            LIMIT 5
        """)

        # Εκτύπωση
        print("\n=== 1. ΜΕΣΗ ΤΑΧΥΤΗΤΑ & DETOUR RATIO ΑΝΑ ΩΡΑ (SQL) ===")
        summary_by_hour.show(24)

        print("\n=== 2. ΟΙ 5 ΤΑΧΥΤΕΡΕΣ ΔΙΑΔΡΟΜΕΣ ===")
        fastest_trips.show()

        print("\n=== 3. ΟΙ 5 ΠΙΟ ΑΡΓΕΣ ΔΙΑΔΡΟΜΕΣ (ΣΥΜΦΟΡΗΣΗ) ===")
        slowest_trips.show()

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
        
    end_time = time.time()
    print("\n" + "="*50)
    print(f"Συνολικός χρόνος εκτέλεσης (SQL): {end_time - start_time} δευτερόλεπτα")
    print("="*50 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()