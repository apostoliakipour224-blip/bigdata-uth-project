import time
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Q6_Spark_SQL").getOrCreate()
    
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    zone_lookup_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/taxi_zone_lookup.csv"
    
    print("\n" + "="*80)
    print("=== ΕΝΑΡΞΗ Q6: SPARK SQL - ΑΝΙΣΟΡΡΟΠΙΑ ΖΩΝΩΝ ===")
    
    try:
        df_trips = spark.read.parquet(trips_path)
        df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_lookup_path)
        
        df_trips.createOrReplaceTempView("trips")
        df_zones.createOrReplaceTempView("zones")
        
        # Το κεντρικό SQL ερώτημα με χρήση CTEs (Common Table Expressions)
        sql_query = """
            WITH filtered_trips AS (
                SELECT pickup_hour, pu_location_id, do_location_id 
                FROM trips 
                WHERE tpep_pickup_datetime IS NOT NULL 
                  AND duration_minutes > 0 
                  AND total_amount > 0
            ),
            pickups_cte AS (
                SELECT pickup_hour, pu_location_id AS location_id, COUNT(*) AS pickups
                FROM filtered_trips
                GROUP BY pickup_hour, pu_location_id
            ),
            dropoffs_cte AS (
                SELECT pickup_hour, do_location_id AS location_id, COUNT(*) AS dropoffs
                FROM filtered_trips
                GROUP BY pickup_hour, do_location_id
            ),
            union_keys AS (
                SELECT pickup_hour, location_id FROM pickups_cte
                UNION
                SELECT pickup_hour, location_id FROM dropoffs_cte
            ),
            metrics_cte AS (
                SELECT 
                    u.pickup_hour,
                    u.location_id,
                    COALESCE(p.pickups, 0) AS pickups,
                    COALESCE(d.dropoffs, 0) AS dropoffs,
                    (COALESCE(p.pickups, 0) + COALESCE(d.dropoffs, 0)) AS activity,
                    (COALESCE(p.pickups, 0) - COALESCE(d.dropoffs, 0)) AS net_pickups,
                    CASE WHEN (COALESCE(p.pickups, 0) + COALESCE(d.dropoffs, 0)) > 0 
                         THEN (COALESCE(p.pickups, 0) - COALESCE(d.dropoffs, 0)) / (COALESCE(p.pickups, 0) + COALESCE(d.dropoffs, 0))
                         ELSE 0 END AS imbalance_ratio
                FROM union_keys u
                LEFT JOIN pickups_cte p ON u.pickup_hour = p.pickup_hour AND u.location_id = p.location_id
                LEFT JOIN dropoffs_cte d ON u.pickup_hour = d.pickup_hour AND u.location_id = d.location_id
            )
            SELECT 
                m.pickup_hour,
                m.location_id,
                z.Borough,
                z.Zone,
                m.pickups,
                m.dropoffs,
                m.activity,
                m.net_pickups,
                ROUND(m.imbalance_ratio, 4) AS imbalance_ratio,
                ROUND(ABS(m.imbalance_ratio), 4) AS abs_imbalance_ratio
            FROM metrics_cte m
            JOIN zones z ON m.location_id = z.LocationID
            WHERE m.activity >= 30
        """
        
        # Εκτέλεση και δημιουργία ενός τελικού View για να τραβάμε τα Top-K
        df_res = spark.sql(sql_query)
        df_res.createOrReplaceTempView("final_metrics")
        
        print("\n--- TOP 5 ΖΩΝΕΣ ΜΕ ΥΨΗΛΟΤΕΡΟ ΘΕΤΙΚΟ NET PICKUPS (SQL) ---")
        spark.sql("SELECT pickup_hour, Borough, Zone, pickups, dropoffs, net_pickups FROM final_metrics ORDER BY net_pickups DESC LIMIT 5").show(truncate=False)
        
        print("\n--- TOP 5 ΖΩΝΕΣ ΜΕ ΠΙΟ ΑΡΝΗΤΙΚΟ NET PICKUPS (SQL) ---")
        spark.sql("SELECT pickup_hour, Borough, Zone, pickups, dropoffs, net_pickups FROM final_metrics ORDER BY net_pickups ASC LIMIT 5").show(truncate=False)

        print("\n--- TOP 5 ΖΩΝΕΣ ΜΕ ΥΨΗΛΟΤΕΡΟ ABS IMBALANCE RATIO (SQL) ---")
        spark.sql("SELECT pickup_hour, Borough, Zone, activity, net_pickups, abs_imbalance_ratio FROM final_metrics ORDER BY abs_imbalance_ratio DESC LIMIT 5").show(truncate=False)

        print("\n--- ΣΥΝΟΨΗ ΑΝΑ PICKUP HOUR (SQL) ---")
        spark.sql("""
            SELECT pickup_hour, 
                   SUM(ABS(net_pickups)) AS total_abs_net_pickups,
                   ROUND(AVG(abs_imbalance_ratio), 4) AS mean_abs_imbalance_ratio
            FROM final_metrics
            GROUP BY pickup_hour
            ORDER BY total_abs_net_pickups DESC
        """).show(24)

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()