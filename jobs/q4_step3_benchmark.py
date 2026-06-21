import time
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Q4_Step3_Benchmark").getOrCreate()
    
    # Διαδρομές στο HDFS (Βεβαιώσου ότι η διαδρομή του CSV είναι η σωστή για το cluster σου)
    parquet_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    csv_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/csv/yellow_tripdata_2024"
    
    print("\n" + "="*80)
    print("=== Q4: ΒΗΜΑ 3 (BENCHMARK CSV VS PARQUET) ===")
    
    # --- 1. ΕΚΤΕΛΕΣΗ ΜΕ PARQUET ---
    print("\n>>> 1. Έναρξη δοκιμής με PARQUET...")
    try:
        start_parquet = time.time()
        
        df_parquet = spark.read.parquet(parquet_path)
        df_parquet.createOrReplaceTempView("trips_parquet")
        
        # Εκτελούμε ένα υποσύνολο του Q4 (π.χ. απλό count και ομαδοποίηση για ένα vendor)
        res_parquet = spark.sql("""
            SELECT hour(CAST(tpep_pickup_datetime AS TIMESTAMP)) as hour, COUNT(*) 
            FROM trips_parquet 
            WHERE vendor_id = 1
            GROUP BY hour
        """)
        # Το .collect() αναγκάζει το Spark να εκτελέσει την πράξη εκείνη τη στιγμή
        res_parquet.collect() 
        
        end_parquet = time.time()
        parquet_time = end_parquet - start_parquet
        print(f" Ολοκληρώθηκε το Parquet σε: {parquet_time:.2f} δευτερόλεπτα")
    except Exception as e:
        print("Σφάλμα στο Parquet:", str(e))
        parquet_time = None

    # --- 2. ΕΚΤΕΛΕΣΗ ΜΕ CSV ---
    print("\n>>> 2. Έναρξη δοκιμής με CSV...")
    try:
        start_csv = time.time()
        
        # Διαβάζουμε το CSV (ορίζουμε header και inferSchema για να είναι δίκαιη η σύγκριση)
        df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        df_csv.createOrReplaceTempView("trips_csv")
        
        res_csv = spark.sql("""
            SELECT hour(CAST(tpep_pickup_datetime AS TIMESTAMP)) as hour, COUNT(*) 
            FROM trips_csv 
            WHERE vendor_id = 1
            GROUP BY hour
        """)
        res_csv.collect()
        
        end_csv = time.time()
        csv_time = end_csv - start_csv
        print(f"Ολοκληρώθηκε το CSV σε: {csv_time:.2f} δευτερόλεπτα")
    except Exception as e:
        print("Σφάλμα στο CSV:", str(e))
        csv_time = None

    # --- ΣΥΓΚΡΙΤΙΚΟ ΣΥΜΠΕΡΑΣΜΑ ---
    if parquet_time and csv_time:
        print("\n" + "="*50)
        print("=== ΤΕΛΙΚΑ ΑΠΟΤΕΛΕΣΜΑΤΑ ΣΥΓΚΡΙΣΗΣ ===")
        print(f"Χρόνος εκτέλεσης με Parquet: {parquet_time:.2f} δευτερόλεπτα")
        print(f"Χρόνος εκτέλεσης με CSV:     {csv_time:.2f} δευτερόλεπτα")
        print(f"Διαφορά: Η ανάγνωση Parquet ήταν {csv_time / parquet_time:.1f} φορές ταχύτερη!")
        print("="*50 + "\n")

    spark.stop()

if __name__ == "__main__":
    main()