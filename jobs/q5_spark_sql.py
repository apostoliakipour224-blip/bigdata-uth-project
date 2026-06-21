import time
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Q5_Spark_SQL").getOrCreate()
    
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    zone_lookup_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/taxi_zone_lookup.csv"
    
    print("\n" + "="*80)
    print("=== ENΑΡΞΗ Q5: SPARK SQL ΥΛΟΠΟΙΗΣΗ ===")
    
    try:
        # Φόρτωση και δημιουργία Views
        spark.read.parquet(trips_path).createOrReplaceTempView("trips")
        spark.read.option("header", "true").option("inferSchema", "true").csv(zone_lookup_path).createOrReplaceTempView("zones")
        
        # Κύριο SQL Query για ροές Borough-to-Borough
        sql_flows = """
        WITH CleanTrips AS (
            SELECT PULocationID, DOLocationID, total_amount, trip_distance, duration_minutes
            FROM trips
            WHERE duration_minutes > 0 AND trip_distance > 0 AND total_amount > 0
        ),
        JoinedTrips AS (
            SELECT 
                pu.Borough AS pu_borough,
                pu.Zone AS pu_zone,
                do.Borough AS do_borough,
                do.Zone AS do_zone,
                t.total_amount,
                t.trip_distance,
                t.duration_minutes,
                CASE 
                    WHEN pu.Zone LIKE '%JFK%' OR pu.Zone LIKE '%LaGuardia%' OR pu.Zone LIKE '%Newark%' OR pu.Zone LIKE '%Airport%' OR pu.Borough = 'EWR' OR
                         do.Zone LIKE '%JFK%' OR do.Zone LIKE '%LaGuardia%' OR do.Zone LIKE '%Newark%' OR do.Zone LIKE '%Airport%' OR do.Borough = 'EWR'
                    THEN 1 ELSE 0 
                END AS is_airport
            FROM CleanTrips t
            JOIN zones pu ON t.PULocationID = pu.LocationID
            JOIN zones do ON t.DOLocationID = do.LocationID
        ),
        TotalCount AS (
            SELECT COUNT(*) as grand_total FROM JoinedTrips
        )
        SELECT 
            pu_borough,
            do_borough,
            COUNT(*) AS trips,
            ROUND(COUNT(*) / (SELECT grand_total FROM TotalCount), 4) AS trip_share,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_total_amount,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
            ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes,
            ROUND(SUM(is_airport) / COUNT(*), 4) AS airport_trip_share
        FROM JoinedTrips
        GROUP BY pu_borough, do_borough
        ORDER BY trips DESC
        """
        
        print("\n--- 1. ΑΠΟΤΕΛΕΣΜΑΤΑ ΡΟΩΝ BOROUGH-TO-BOROUGH (SQL) ---")
        spark.sql(sql_flows).show(20, truncate=False)
        
        # SQL Query ξεχωριστά για τις Top-K διαδρομές αεροδρομίου (pu_zone -> do_zone)
        sql_airport_routes = """
        WITH AirportTrips AS (
            SELECT 
                pu.Zone AS pu_zone,
                do.Zone AS do_zone,
                t.airport_fee,
                t.total_amount,
                t.duration_minutes,
                t.trip_distance
            FROM trips t
            JOIN zones pu ON t.PULocationID = pu.LocationID
            JOIN zones do ON t.DOLocationID = do.LocationID
            WHERE t.duration_minutes > 0 AND t.trip_distance > 0 AND t.total_amount > 0
              AND (pu.Zone LIKE '%JFK%' OR pu.Zone LIKE '%LaGuardia%' OR pu.Zone LIKE '%Newark%' OR pu.Zone LIKE '%Airport%' OR pu.Borough = 'EWR' OR
                   do.Zone LIKE '%JFK%' OR do.Zone LIKE '%LaGuardia%' OR do.Zone LIKE '%Newark%' OR do.Zone LIKE '%Airport%' OR do.Borough = 'EWR')
        )
        SELECT 
            pu_zone,
            do_zone,
            COUNT(*) AS trips,
            ROUND(AVG(CAST(airport_fee AS DOUBLE)), 2) AS avg_airport_fee,
            ROUND(AVG(total_amount), 2) AS avg_total_amount,
            ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance
        FROM AirportTrips
        GROUP BY pu_zone, do_zone
        ORDER BY trips DESC
        LIMIT 10
        """
        
        print("\n--- 2. TOP-10 ΔΙΑΔΡΟΜΕΣ ΑΕΡΟΔΡΟΜΙΩΝ (ΖΩΝΗ προς ΖΩΝΗ) ---")
        spark.sql(sql_airport_routes).show(truncate=False)

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()