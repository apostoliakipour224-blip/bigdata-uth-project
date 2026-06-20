import time
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Q4_Step1_Robust").getOrCreate()
    
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    
    print("\n" + "="*70)
    print("=== ENΑΡΞΗ Q4: ΒΗΜΑ 1 (ROBUST SQL) ===")
    
    try:
        df_trips = spark.read.parquet(trips_path)
        df_trips.createOrReplaceTempView("trips")
        
        # SQL Query που εξασφαλίζει τη σωστή μορφή των δεδομένων (CAST)
        sql_query = """
        WITH CleanTypes AS (
            SELECT 
                -- Μετατροπή ημερομηνιών σε TIMESTAMP για να πιάσει σίγουρα την ώρα
                hour(CAST(tpep_pickup_datetime AS TIMESTAMP)) AS pickup_hour,
                -- Μετατροπή πληρωμής σε ακέραιο (INT) για να πιάσει το 1 και το 2
                CAST(payment_type AS INT) AS p_type,
                fare_amount,
                total_amount,
                tip_amount,
                trip_distance,
                (unix_timestamp(CAST(tpep_dropoff_datetime AS TIMESTAMP)) - unix_timestamp(CAST(tpep_pickup_datetime AS TIMESTAMP))) / 60.0 AS duration
            FROM trips
        ),
        FilteredTrips AS (
            SELECT * FROM CleanTypes
            WHERE trip_distance > 0 
              AND fare_amount > 0 
              AND total_amount > 0
              AND duration > 0
              AND p_type IN (1, 2)
              AND pickup_hour IS NOT NULL
        ),
        HourlyStats AS (
            SELECT 
                pickup_hour,
                p_type AS payment_type,
                COUNT(*) AS trips,
                AVG(fare_amount) AS avg_fare_amount,
                AVG(total_amount) AS avg_total_amount,
                AVG(tip_amount) AS avg_tip_amount,
                AVG(tip_amount / fare_amount) AS tip_rate,
                SUM(CASE WHEN tip_amount = 0 THEN 1 ELSE 0 END) / COUNT(*) AS zero_tip_share,
                AVG(trip_distance) AS avg_trip_distance,
                AVG(duration) AS avg_duration_minutes
            FROM FilteredTrips
            GROUP BY pickup_hour, p_type
        )
        SELECT 
            pickup_hour,
            CASE WHEN payment_type = 1 THEN 'Card' ELSE 'Cash' END AS payment_method,
            trips,
            ROUND(trips / SUM(trips) OVER (PARTITION BY pickup_hour), 4) AS payment_share_in_hour,
            ROUND(avg_fare_amount, 2) AS avg_fare_amount,
            ROUND(avg_total_amount, 2) AS avg_total_amount,
            ROUND(avg_tip_amount, 2) AS avg_tip_amount,
            ROUND(tip_rate, 4) AS tip_rate,
            ROUND(zero_tip_share, 4) AS zero_tip_share,
            ROUND(avg_trip_distance, 2) AS avg_trip_distance,
            ROUND(avg_duration_minutes, 2) AS avg_duration_minutes
        FROM HourlyStats
        ORDER BY pickup_hour, payment_method
        """
        
        start_time = time.time()
        
        df_q4 = spark.sql(sql_query)
        # Εμφάνιση 48 γραμμών (24 ώρες x 2 τρόποι πληρωμής)
        df_q4.show(48, truncate=False) 
        
        end_time = time.time()
        print(f"\nΧρόνος εκτέλεσης Βήματος 1: {end_time - start_time:.2f} δευτερόλεπτα")

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
        
    finally:
        print("="*70 + "\n")
        spark.stop()

if __name__ == "__main__":
    main()