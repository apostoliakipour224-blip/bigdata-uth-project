import time
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Q5_Spark_SQL").getOrCreate()
    
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    zone_lookup_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/taxi_zone_lookup.csv"
    
    print("\n" + "="*80)
    print("=== ΕΝΑΡΞΗ Q5: SPARK SQL & ΑΝΑΛΥΣΗ ΑΕΡΟΔΡΟΜΙΩΝ ===")
    
    try:
        df_trips = spark.read.parquet(trips_path)
        df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_lookup_path)
        
        # Καταχώρηση των DataFrames ως SQL Views
        df_trips.createOrReplaceTempView("trips")
        df_zones.createOrReplaceTempView("zones")
        
        # 1. Φιλτράρισμα και δημιουργία νέου View
        spark.sql("""
            SELECT * FROM trips 
            WHERE duration_minutes > 0 
              AND trip_distance > 0 
              AND total_amount > 0
        """).createOrReplaceTempView("trips_filtered")
        
        total_trips = spark.sql("SELECT COUNT(*) FROM trips_filtered").collect()[0][0]
        
        # 2. Το κεντρικό ερώτημα SQL για τις Ροές Borough-to-Borough
        print("\n>>> Εκτέλεση SQL: Ροές Borough-to-Borough (Ισοδύναμο του DF API)...")
        start_time = time.time()
        
        sql_flows = f"""
            SELECT 
                pu.Borough AS pu_borough,
                do.Borough AS do_borough,
                COUNT(*) AS trips,
                ROUND(COUNT(*) / {total_trips}, 4) AS trip_share,
                ROUND(SUM(t.total_amount), 2) AS total_revenue,
                ROUND(AVG(t.total_amount), 2) AS avg_total_amount,
                ROUND(AVG(t.trip_distance), 2) AS avg_trip_distance,
                ROUND(AVG(t.duration_minutes), 2) AS avg_duration_minutes,
                ROUND(SUM(CASE WHEN 
                    pu.Zone LIKE '%JFK%' OR pu.Zone LIKE '%LaGuardia%' OR pu.Zone LIKE '%Newark%' OR pu.Zone LIKE '%Airport%' OR
                    do.Zone LIKE '%JFK%' OR do.Zone LIKE '%LaGuardia%' OR do.Zone LIKE '%Newark%' OR do.Zone LIKE '%Airport%' OR
                    pu.Borough = 'EWR' OR do.Borough = 'EWR' 
                    THEN 1 ELSE 0 END) / COUNT(*), 4) AS airport_trip_share
            FROM trips_filtered t
            JOIN zones pu ON t.pu_location_id = pu.LocationID
            JOIN zones do ON t.do_location_id = do.LocationID
            GROUP BY pu.Borough, do.Borough
            ORDER BY trips DESC
        """
        df_sql_flows = spark.sql(sql_flows)
        df_sql_flows.show(20, truncate=False)
        
        end_time = time.time()
        print(f"Χρόνος εκτέλεσης Spark SQL (Borough Flows): {end_time - start_time:.2f} δευτερόλεπτα")

        # 3. Το ερώτημα SQL αποκλειστικά για τις Διαδρομές Αεροδρομίου (Zone-to-Zone)
        print("\n" + "-"*50)
        print(">>> Εκτέλεση SQL: Αναλυτικές Διαδρομές Αεροδρομίων (Top 20 Zone-to-Zone)...")
        
        sql_airport = """
            WITH airport_trips AS (
                SELECT 
                    t.*, 
                    pu.Zone AS pu_zone, 
                    do.Zone AS do_zone
                FROM trips_filtered t
                JOIN zones pu ON t.pu_location_id = pu.LocationID
                JOIN zones do ON t.do_location_id = do.LocationID
                WHERE 
                    pu.Zone LIKE '%JFK%' OR pu.Zone LIKE '%LaGuardia%' OR pu.Zone LIKE '%Newark%' OR pu.Zone LIKE '%Airport%' OR
                    do.Zone LIKE '%JFK%' OR do.Zone LIKE '%LaGuardia%' OR do.Zone LIKE '%Newark%' OR do.Zone LIKE '%Airport%' OR
                    pu.Borough = 'EWR' OR do.Borough = 'EWR'
            )
            SELECT 
                pu_zone,
                do_zone,
                COUNT(*) AS trips,
                ROUND(AVG(Airport_fee), 2) AS avg_airport_fee,
                ROUND(AVG(total_amount), 2) AS avg_total_amount,
                ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes,
                ROUND(AVG(trip_distance), 2) AS avg_trip_distance
            FROM airport_trips
            GROUP BY pu_zone, do_zone
            ORDER BY trips DESC
        """
        df_airport_flows = spark.sql(sql_airport)
        df_airport_flows.show(20, truncate=False)

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()