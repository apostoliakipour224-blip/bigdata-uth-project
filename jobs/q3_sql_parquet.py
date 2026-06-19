import time
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Q3_Spark_SQL_Parquet").getOrCreate()
    
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    zone_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/taxi_zone_lookup.csv"
    
    start_time = time.time()
    print("\n" + "="*50)
    print("=== ENΑΡΞΗ ΥΠΟΛΟΓΙΣΜΟΥ Q3 (SPARK SQL - PARQUET) ===")
    
    try:
        # Αντί για: df_trips = spark.read.parquet(trips_path)
# Βάλε:
        # Φόρτωση του Parquet με περιορισμό για δοκιμή
        df_trips = spark.read.parquet(trips_path).limit(100000)
        df_trips.cache()  # Κρατάμε το δείγμα στη μνήμη
        df_trips.createOrReplaceTempView("trips")
        
        df_zones = spark.read.csv(zone_path, header=True, inferSchema=True)
        df_zones.createOrReplaceTempView("zones")
        
        available_columns = [col.lower() for col in df_trips.columns]
        surcharge_candidates = ["extra", "mta_tax", "tolls_amount", "improvement_surcharge", "congestion_surcharge", "airport_fee"]
        
        valid_surcharges = [c for c in surcharge_candidates if c in available_columns]
        if valid_surcharges:
            surcharge_expr = " + ".join([f"COALESCE({c}, 0)" for c in valid_surcharges])
        else:
            surcharge_expr = "0"
            
        sql_query = f"""
        WITH FilteredTrips AS (
            SELECT *,
                   (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0 AS calc_duration
            FROM trips
            WHERE trip_distance > 0 
              AND fare_amount > 0 
              AND total_amount > 0
        ),
        CleanedTrips AS (
            SELECT * FROM FilteredTrips WHERE calc_duration > 0
        ),
        BucketedTrips AS (
            SELECT t.*, 
                   z.Borough AS pickup_borough, 
                   z.Zone AS pickup_zone,
                   CASE 
                       WHEN t.trip_distance < 1 THEN 'very_short'
                       WHEN t.trip_distance >= 1 AND t.trip_distance < 3 THEN 'short'
                       WHEN t.trip_distance >= 3 AND t.trip_distance < 10 THEN 'medium'
                       ELSE 'long'
                   END AS distance_bucket,
                   ({surcharge_expr}) AS total_surcharges
            FROM CleanedTrips t
            -- ΕΔΩ ΕΓΙΝΕ Η ΔΙΟΡΘΩΣΗ ΤΟΥ ΟΝΟΜΑΤΟΣ:
            JOIN zones z ON t.pu_location_id = z.LocationID
        ),
        AggregatedZones AS (
            SELECT pickup_borough,
                   pickup_zone,
                   distance_bucket,
                   COUNT(total_amount) AS trips,
                   SUM(total_amount) AS total_revenue,
                   AVG(total_amount) AS avg_revenue_per_trip,
                   SUM(total_amount) / SUM(trip_distance) AS revenue_per_mile,
                   SUM(total_amount) / SUM(calc_duration) AS revenue_per_minute,
                   AVG(fare_amount) AS avg_fare_amount,
                   AVG(tip_amount) AS avg_tip_amount,
                   SUM(total_surcharges) / SUM(total_amount) AS surcharge_share
            FROM BucketedTrips
            GROUP BY pickup_borough, pickup_zone, distance_bucket
        )
        SELECT * FROM AggregatedZones
        """
        
        df_aggregated = spark.sql(sql_query)
        df_aggregated.createOrReplaceTempView("aggregated_results")
        
        MIN_TRIPS = 50
        K = 5
        
        print(f"\n=== TOP {K} ΖΩΝΕΣ ΒΑΣΕΙ TOTAL REVENUE (ΧΩΡΙΣ ΟΡΙΟ) ===")
        spark.sql(f"""
            SELECT pickup_borough, pickup_zone, distance_bucket, trips, ROUND(total_revenue, 2) AS total_revenue
            FROM aggregated_results
            ORDER BY total_revenue DESC
            LIMIT {K}
        """).show()
        
        print(f"\n=== TOP {K} ΖΩΝΕΣ ΒΑΣΕΙ REVENUE PER MILE (ΜΕ ΟΡΙΟ TRIPS >= {MIN_TRIPS}) ===")
        spark.sql(f"""
            SELECT pickup_borough, pickup_zone, distance_bucket, trips, ROUND(revenue_per_mile, 2) AS rev_per_mile
            FROM aggregated_results
            WHERE trips >= {MIN_TRIPS}
            ORDER BY revenue_per_mile DESC
            LIMIT {K}
        """).show()
        
        print(f"\n=== TOP {K} ΖΩΝΕΣ ΒΑΣΕΙ REVENUE PER MINUTE (ΜΕ ΟΡΙΟ TRIPS >= {MIN_TRIPS}) ===")
        spark.sql(f"""
            SELECT pickup_borough, pickup_zone, distance_bucket, trips, ROUND(revenue_per_minute, 2) AS rev_per_min
            FROM aggregated_results
            WHERE trips >= {MIN_TRIPS}
            ORDER BY revenue_per_minute DESC
            LIMIT {K}
        """).show()
        
        print("\n=== ΣΧΕΔΙΟ ΕΚΤΕΛΕΣΗΣ (EXPLAIN FORMATTED) ΓΙΑ ΤΟΝ ΕΛΕΓΧΟ PARTITION PRUNING ===")
        df_aggregated.explain(mode="formatted")

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
        
    end_time = time.time()
    print("\n" + "="*50)
    print(f"Συνολικός χρόνος εκτέλεσης (Parquet - SQL): {end_time - start_time} δευτερόλεπτα")
    print("="*50 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()