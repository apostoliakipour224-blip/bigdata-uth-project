import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("Q4_Step2_Comparative").getOrCreate()
    
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    
    print("\n" + "="*80)
    print("=== Q4: ΒΗΜΑ 2 (COMPARATIVE TABLE & VENDORS) ===")
    
    try:
        df_trips = spark.read.parquet(trips_path)
        df_trips.createOrReplaceTempView("trips")
        
        # Καθαρισμός δεδομένων (όπως στο Βήμα 1)
        spark.sql("""
            SELECT 
                hour(CAST(tpep_pickup_datetime AS TIMESTAMP)) AS pickup_hour,
                vendor_id,
                CAST(payment_type AS INT) AS p_type,
                fare_amount,
                total_amount,
                tip_amount,
                (tip_amount / fare_amount) AS tip_rate,
                trip_distance
            FROM trips
            WHERE trip_distance > 0 
              AND fare_amount > 0 
              AND total_amount > 0
              AND CAST(payment_type AS INT) IN (1, 2)
              AND hour(CAST(tpep_pickup_datetime AS TIMESTAMP)) IS NOT NULL
        """).createOrReplaceTempView("clean_trips")

        # ---------------------------------------------------------
        # ΥΛΟΠΟΙΗΣΗ 1: SPARK SQL (Κύρια υλοποίηση με CASE WHEN)
        # ---------------------------------------------------------
        print("\n--- 1. SPARK SQL: Πίνακας card_vs_cash ανά ώρα ---")
        sql_card_vs_cash = """
            SELECT 
                pickup_hour,
                SUM(CASE WHEN p_type = 1 THEN 1 ELSE 0 END) AS card_trips,
                SUM(CASE WHEN p_type = 2 THEN 1 ELSE 0 END) AS cash_trips,
                ROUND(SUM(CASE WHEN p_type = 1 THEN 1 ELSE 0 END) / COUNT(*), 4) AS card_share,
                ROUND(AVG(CASE WHEN p_type = 1 THEN total_amount ELSE NULL END), 2) AS avg_total_card,
                ROUND(AVG(CASE WHEN p_type = 2 THEN total_amount ELSE NULL END), 2) AS avg_total_cash,
                ROUND(AVG(CASE WHEN p_type = 1 THEN fare_amount ELSE NULL END), 2) AS avg_fare_card,
                ROUND(AVG(CASE WHEN p_type = 2 THEN fare_amount ELSE NULL END), 2) AS avg_fare_cash,
                ROUND(AVG(CASE WHEN p_type = 1 THEN tip_rate ELSE NULL END), 4) AS avg_tip_rate_card
            FROM clean_trips
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """
        start_sql = time.time()
        df_sql_result = spark.sql(sql_card_vs_cash)
        df_sql_result.show(24, truncate=False)
        print(f"Χρόνος Spark SQL: {time.time() - start_sql:.2f} δευτερόλεπτα")

        # ---------------------------------------------------------
        # ΥΛΟΠΟΙΗΣΗ 2: DATAFRAME API (Ισοδύναμη υλοποίηση)
        # ---------------------------------------------------------
        print("\n--- 2. DATAFRAME API: Πίνακας card_vs_cash ανά ώρα ---")
        start_df = time.time()
        
        df_clean = spark.table("clean_trips")
        
        df_api_result = df_clean.groupBy("pickup_hour").agg(
            F.sum(F.when(F.col("p_type") == 1, 1).otherwise(0)).alias("card_trips"),
            F.sum(F.when(F.col("p_type") == 2, 1).otherwise(0)).alias("cash_trips"),
            F.round(F.sum(F.when(F.col("p_type") == 1, 1).otherwise(0)) / F.count("*"), 4).alias("card_share"),
            F.round(F.avg(F.when(F.col("p_type") == 1, F.col("total_amount"))), 2).alias("avg_total_card"),
            F.round(F.avg(F.when(F.col("p_type") == 2, F.col("total_amount"))), 2).alias("avg_total_cash"),
            F.round(F.avg(F.when(F.col("p_type") == 1, F.col("fare_amount"))), 2).alias("avg_fare_card"),
            F.round(F.avg(F.when(F.col("p_type") == 2, F.col("fare_amount"))), 2).alias("avg_fare_cash"),
            F.round(F.avg(F.when(F.col("p_type") == 1, F.col("tip_rate"))), 4).alias("avg_tip_rate_card")
        ).orderBy("pickup_hour")
        
        df_api_result.show(24, truncate=False)
        print(f"Χρόνος DataFrame API: {time.time() - start_df:.2f} δευτερόλεπτα")

        # ---------------------------------------------------------
        # ΣΥΝΟΨΗ ΑΝΑ VENDOR (Ερώτημα γ)
        # ---------------------------------------------------------
        print("\n--- 3. ΣΥΝΟΨΗ ΑΝΑ VENDOR_ID ---")
        sql_vendors = """
            SELECT 
                vendor_id,
                ROUND(SUM(CASE WHEN p_type = 1 THEN 1 ELSE 0 END) / COUNT(*), 4) AS card_share,
                ROUND(AVG(CASE WHEN p_type = 1 THEN tip_rate ELSE NULL END), 4) AS avg_tip_rate_card
            FROM clean_trips
            GROUP BY vendor_id
            ORDER BY vendor_id
        """
        spark.sql(sql_vendors).show(truncate=False)

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
        
    finally:
        print("="*80 + "\n")
        spark.stop()

if __name__ == "__main__":
    main()