import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("Q5_DataFrame_API").getOrCreate()
    
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    zone_lookup_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/taxi_zone_lookup.csv"
    
    print("\n" + "="*80)
    print("=== ENΑΡΞΗ Q5: DATAFRAME API & JOIN OPTIMIZATION ===")
    
    try:
        df_trips = spark.read.parquet(trips_path)
        df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_lookup_path)
        
        df_trips_filtered = df_trips.filter(
            (F.col("duration_minutes") > 0) & 
            (F.col("trip_distance") > 0) & 
            (F.col("total_amount") > 0)
        )
        
        df_pu_zone = df_zones.select(
            F.col("LocationID").alias("pu_id"),
            F.col("Borough").alias("pu_borough"),
            F.col("Zone").alias("pu_zone")
        )
        
        df_do_zone = df_zones.select(
            F.col("LocationID").alias("do_id"),
            F.col("Borough").alias("do_borough"),
            F.col("Zone").alias("do_zone")
        )
        
        print("\n>>> [Δοκιμή 1] Εκτέλεση με Προεπιλεγμένο Σχέδιο (Broadcast Hash Join)...")
        start_default = time.time()
        
        df_joined_default = df_trips_filtered \
            .join(df_pu_zone, F.col("pu_location_id") == F.col("pu_id"), "inner") \
            .join(df_do_zone, F.col("do_location_id") == F.col("do_id"), "inner")
            
        airport_condition = (
            F.col("pu_zone").contains("JFK") | F.col("pu_zone").contains("LaGuardia") | 
            F.col("pu_zone").contains("Newark") | F.col("pu_zone").contains("Airport") |
            F.col("do_zone").contains("JFK") | F.col("do_zone").contains("LaGuardia") | 
            F.col("do_zone").contains("Newark") | F.col("do_zone").contains("Airport") |
            (F.col("pu_borough") == "EWR") | (F.col("do_borough") == "EWR")
        )
        
        df_with_airport = df_joined_default.withColumn("is_airport", F.when(airport_condition, 1).otherwise(0))
        
        total_trips = df_with_airport.count()
        
        df_flows = df_with_airport.groupBy("pu_borough", "do_borough").agg(
            F.count("*").alias("trips"),
            F.round(F.count("*") / F.lit(total_trips), 4).alias("trip_share"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.sum("is_airport") / F.count("*"), 4).alias("airport_trip_share")
        ).orderBy(F.desc("trips"))
        
        print("\n--- ΠΙΝΑΚΑΣ ΡΟΩΝ BOROUGH-TO-BOROUGH (Top 20) ---")
        df_flows.show(20, truncate=False)
        
        end_default = time.time()
        print(f"Χρόνος προεπιλεγμένης εκτέλεσης: {end_default - start_default:.2f} δευτερόλεπτα")
        
        print("\n--- ΦΥΣΙΚΟ ΣΧΕΔΙΟ (DEFAULT JOIN) ---")
        df_flows.explain()

        print("\n" + "-"*50)
        print(">>> [Δοκιμή 2] Απενεργοποίηση Broadcast Join (Αναγκαστικό Sort-Merge Join)...")
        
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        
        start_smj = time.time()
        
        df_joined_smj = df_trips_filtered \
            .join(df_pu_zone, F.col("pu_location_id") == F.col("pu_id"), "inner") \
            .join(df_do_zone, F.col("do_location_id") == F.col("do_id"), "inner")
            
        df_with_airport_smj = df_joined_smj.withColumn("is_airport", F.when(airport_condition, 1).otherwise(0))
        
        df_flows_smj = df_with_airport_smj.groupBy("pu_borough", "do_borough").agg(
            F.count("*").alias("trips")
        ).orderBy(F.desc("trips"))
        
        df_flows_smj.collect()
        
        end_smj = time.time()
        print(f"Χρόνος εκτέλεσης με Sort-Merge Join: {end_smj - start_smj:.2f} δευτερόλεπτα")
        
        print("\n--- ΦΥΣΙΚΟ ΣΧΕΔΙΟ (SORT-MERGE JOIN) ---")
        df_flows_smj.explain()

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()