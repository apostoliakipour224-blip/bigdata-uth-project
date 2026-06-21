import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("Q6_DataFrame_API").getOrCreate()
    
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    zone_lookup_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/taxi_zone_lookup.csv"
    
    print("\n" + "="*80)
    print("=== ΕΝΑΡΞΗ Q6: DATAFRAME API - ΑΝΙΣΟΡΡΟΠΙΑ ΖΩΝΩΝ ===")
    
    try:
        df_trips = spark.read.parquet(trips_path)
        df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_lookup_path)
        
        # Φιλτράρισμα έγκυρων εγγραφών
        df_filtered = df_trips.filter(
            F.col("tpep_pickup_datetime").isNotNull() & 
            (F.col("duration_minutes") > 0) & 
            (F.col("total_amount") > 0)
        )
        
        # Σύνολο Pickups
        pickups = df_filtered.groupBy("pickup_hour", "pu_location_id") \
            .agg(F.count("*").alias("pickups")) \
            .withColumnRenamed("pu_location_id", "location_id")
            
        # Σύνολο Dropoffs (με βάση την ώρα επιβίβασης)
        dropoffs = df_filtered.groupBy("pickup_hour", "do_location_id") \
            .agg(F.count("*").alias("dropoffs")) \
            .withColumnRenamed("do_location_id", "location_id")
            
        # Full Outer Join
        joined = pickups.join(dropoffs, ["pickup_hour", "location_id"], "full").na.fill(0)
        
        # Υπολογισμός μετρικών ανισορροπίας
        metrics_df = joined.withColumn("activity", F.col("pickups") + F.col("dropoffs")) \
            .withColumn("net_pickups", F.col("pickups") - F.col("dropoffs")) \
            .withColumn("imbalance_ratio", F.when(F.col("activity") > 0, (F.col("pickups") - F.col("dropoffs")) / F.col("activity")).otherwise(0)) \
            .withColumn("abs_imbalance_ratio", F.abs(F.col("imbalance_ratio")))
            
        # Συνένωση με taxi_zone_lookup
        final_df = metrics_df.join(df_zones, metrics_df.location_id == df_zones.LocationID, "inner")
        
        # Εφαρμογή ορίου δραστηριότητας (activity >= 30)
        filtered_final = final_df.filter(F.col("activity") >= 30)
        
        # 1. Top-5 ζώνες με υψηλότερο θετικό net_pickups (Πλεόνασμα ζήτησης)
        print("\n--- TOP 5 ΖΩΝΕΣ ΜΕ ΥΨΗΛΟΤΕΡΟ ΘΕΤΙΚΟ NET PICKUPS ---")
        filtered_final.orderBy(F.desc("net_pickups")).select("pickup_hour", "Borough", "Zone", "pickups", "dropoffs", "net_pickups").show(5, truncate=False)
        
        # 2. Top-5 ζώνες με πιο αρνητικό net_pickups (Συσσώρευση ταξί)
        print("\n--- TOP 5 ΖΩΝΕΣ ΜΕ ΠΙΟ ΑΡΝΗΤΙΚΟ NET PICKUPS ---")
        filtered_final.orderBy(F.asc("net_pickups")).select("pickup_hour", "Borough", "Zone", "pickups", "dropoffs", "net_pickups").show(5, truncate=False)
        
        # 3. Top-5 ζώνες με υψηλότερο abs_imbalance_ratio
        print("\n--- TOP 5 ΖΩΝΕΣ ΜΕ ΥΨΗΛΟΤΕΡΟ ABS IMBALANCE RATIO ---")
        filtered_final.orderBy(F.desc("abs_imbalance_ratio")).select("pickup_hour", "Borough", "Zone", "activity", "net_pickups", "abs_imbalance_ratio").show(5, truncate=False)
        
        # 4. Σύνοψη ανά pickup_hour
        print("\n--- ΣΥΝΟΨΗ ΑΝΑ PICKUP HOUR ---")
        hourly_summary = filtered_final.groupBy("pickup_hour").agg(
            F.sum(F.abs(F.col("net_pickups"))).alias("total_abs_net_pickups"),
            F.round(F.avg("abs_imbalance_ratio"), 4).alias("mean_abs_imbalance_ratio")
        ).orderBy(F.desc("total_abs_net_pickups"))
        hourly_summary.show(24)
        
    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()