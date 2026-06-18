import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col  # ΝΕΟ: Εισαγωγή της συνάρτησης col

def main():
    # 1. Αρχικοποίηση του Spark Session
    spark = SparkSession.builder \
        .appName("Q1_RDD_API") \
        .getOrCreate()

    start_time = time.time()

    # ΠΡΟΣΟΧΗ: Βάλε εδώ το ακριβές path που είχες βρει και δούλεψε στα προηγούμενα!
    parquet_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
    
    df = spark.read.parquet(parquet_path)
    
    # 2. ΝΕΟ: Αναγκαστική μετατροπή των στηλών σε μορφή Timestamp πριν γίνουν RDD
    df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
           .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
    
    # Μετατροπή σε RDD
    raw_rdd = df.rdd

    # 3. Συνάρτηση Map
    def extract_and_filter(row):
        pickup_dt = row.tpep_pickup_datetime
        dropoff_dt = row.tpep_dropoff_datetime
        
        if not pickup_dt or not dropoff_dt:
            return None
            
        hour = pickup_dt.hour
        day = pickup_dt.day
        
        if 3 <= hour <= 6:
            duration = (dropoff_dt - pickup_dt).total_seconds() / 60.0
            pass_count = float(row.passenger_count or 0.0)
            dist = float(row.trip_distance or 0.0)
            total = float(row.total_amount or 0.0)
            pu_loc = row.pu_location_id
            
            return ((day, hour), (1, {pu_loc} if pu_loc else set(), pass_count, duration, dist, total))
        return None

    mapped_rdd = raw_rdd.map(extract_and_filter).filter(lambda x: x is not None)

    # 4. Υπολογισμός συνολικών trips
    mapped_rdd.cache()
    total_trips_in_window = mapped_rdd.map(lambda x: x[1][0]).sum()

    # 5. Ομαδοποίηση
    zero_val = (0, set(), 0.0, 0.0, 0.0, 0.0)

    def seq_op(acc, val):
        return (
            acc[0] + val[0],
            acc[1].union(val[1]),
            acc[2] + val[2],
            acc[3] + val[3],
            acc[4] + val[4],
            acc[5] + val[5]
        )

    def comb_op(acc1, acc2):
        return (
            acc1[0] + acc2[0],
            acc1[1].union(acc2[1]),
            acc1[2] + acc2[2],
            acc1[3] + acc2[3],
            acc1[4] + acc2[4],
            acc1[5] + acc2[5]
        )

    aggregated_rdd = mapped_rdd.aggregateByKey(zero_val, seq_op, comb_op)

    # 6. Υπολογισμός τελικών μέσων όρων
    def calculate_averages(x):
        key, acc = x
        day, hour = key
        trips = acc[0]
        unique_zones = len(acc[1])
        
        avg_pass = acc[2] / trips if trips > 0 else 0
        avg_dur = acc[3] / trips if trips > 0 else 0
        avg_dist = acc[4] / trips if trips > 0 else 0
        avg_total = acc[5] / trips if trips > 0 else 0
        total_revenue = acc[5]
        
        percentage = (trips * 100.0) / total_trips_in_window if total_trips_in_window > 0 else 0
        
        return (trips, (day, hour, trips, unique_zones, avg_pass, avg_dur, avg_dist, avg_total, total_revenue, percentage))

    final_rdd = aggregated_rdd.map(calculate_averages)

    # 7. Ταξινόμηση
    top_12 = final_rdd.sortByKey(ascending=False).take(12)

    end_time = time.time()

    # 8. Εμφάνιση
    print("\n" + "="*150)
    print(f"| {'pickup_date':>11} | {'pickup_hour':>11} | {'trips':>7} | {'unique_zones':>12} | {'avg_passenger':>13} | {'avg_duration':>12} | {'avg_distance':>12} | {'avg_total':>10} | {'total_revenue':>13} | {'share_%':>8} |")
    print("="*150)

    for item in top_12:
        val = item[1]
        print(f"| {val[0]:>11} | {val[1]:>11} | {val[2]:>7} | {val[3]:>12} | {val[4]:>13.4f} | {val[5]:>12.4f} | {val[6]:>12.4f} | {val[7]:>10.4f} | {val[8]:>13.2f} | {val[9]:>8.4f} |")

    print("="*150)
    print(f"\nRDD API Execution Time: {end_time - start_time:.2f} seconds\n")

    spark.stop()

if __name__ == "__main__":
    main()