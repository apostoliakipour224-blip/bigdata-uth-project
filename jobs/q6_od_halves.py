import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("Q6_OD_Halves_Experiment").getOrCreate()
    
    # Διαβάζουμε όλο το σύνολο δεδομένων
    trips_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    
    print("\n" + "="*80)
    print("=== ΕΝΑΡΞΗ Q6: ΠΕΙΡΑΜΑ ΚΛΙΜΑΚΩΣΗΣ (OD-HALVES) ===")
    start_total_time = time.time()
    
    try:
        df = spark.read.parquet(trips_path)
        
        # 1. Φίλτρα Εγκυρότητας και εξαγωγή Ημέρας
        df_valid = df.filter(
            (F.col("duration_minutes") > 0) & 
            (F.col("trip_distance") > 0) & 
            (F.col("fare_amount") > 0)
        ).withColumn("pickup_day", F.dayofmonth("tpep_pickup_datetime"))
        
        # 2. Διαχωρισμός στα Δύο Ημίχρονα (H1 & H2)
        h1_df = df_valid.filter(F.col("pickup_day") <= 15)
        h2_df = df_valid.filter(F.col("pickup_day") > 15)
        
        # 3. Ομαδοποίηση και Αθροίσματα για H1
        h1_agg = h1_df.groupBy("pu_location_id", "do_location_id", "pickup_hour").agg(
            F.count("*").alias("trips_h1"),
            F.avg("fare_amount").alias("avg_fare_h1"),
            F.avg("total_amount").alias("avg_total_h1")
        )
        
        # Ομαδοποίηση και Αθροίσματα για H2
        h2_agg = h2_df.groupBy("pu_location_id", "do_location_id", "pickup_hour").agg(
            F.count("*").alias("trips_h2"),
            F.avg("fare_amount").alias("avg_fare_h2"),
            F.avg("total_amount").alias("avg_total_h2")
        )
        
        # 4. Inner Join στα κλειδιά (Εδώ γίνεται το βαρύ computation)
        joined_df = h1_agg.join(h2_agg, ["pu_location_id", "do_location_id", "pickup_hour"], "inner")
        
        # 5. Υπολογισμός Ποσοστιαίων Αλλαγών
        # Βάζουμε φίλτρο να υπάρχουν τουλάχιστον 50 κούρσες στο H1 για να αποφύγουμε στατιστικά ασήμαντα ζεύγη (π.χ. από 1 κούρσα πήγε 2 = 100% αύξηση)
        metrics_df = joined_df.filter(F.col("trips_h1") >= 50) \
            .withColumn("trips_change_pct", F.round(((F.col("trips_h2") - F.col("trips_h1")) * 100) / F.col("trips_h1"), 2)) \
            .withColumn("fare_change_pct", F.round(((F.col("avg_fare_h2") - F.col("avg_fare_h1")) * 100) / F.col("avg_fare_h1"), 2))
            
        metrics_df.cache() # Κάνουμε cache για να τρέξουν γρήγορα τα δύο show() από κάτω
        metrics_df.count() # Force action
        
        print("\n--- TOP 5 ΖΕΥΓΗ ΜΕ ΜΕΓΑΛΥΤΕΡΗ ΠΟΣΟΣΤΙΑΙΑ ΑΥΞΗΣΗ ΔΙΑΔΡΟΜΩΝ (H1 -> H2) ---")
        metrics_df.orderBy(F.desc("trips_change_pct")).select("pu_location_id", "do_location_id", "pickup_hour", "trips_h1", "trips_h2", "trips_change_pct").show(5)
        
        print("\n--- TOP 5 ΖΕΥΓΗ ΜΕ ΜΕΓΑΛΥΤΕΡΗ ΠΟΣΟΣΤΙΑΙΑ ΜΕΙΩΣΗ ΔΙΑΔΡΟΜΩΝ (H1 -> H2) ---")
        metrics_df.orderBy(F.asc("trips_change_pct")).select("pu_location_id", "do_location_id", "pickup_hour", "trips_h1", "trips_h2", "trips_change_pct").show(5)

        end_total_time = time.time()
        print(f"\nΣΥΝΟΛΙΚΟΣ ΧΡΟΝΟΣ ΕΚΤΕΛΕΣΗΣ (Driver Time): {end_total_time - start_total_time:.2f} δευτερόλεπτα")
        
        # Για να δούμε τα σχέδια εκτέλεσης (όπως ζητάει το ερώτημα δ)
        metrics_df.explain("formatted")

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()