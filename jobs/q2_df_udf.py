import time
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, round, udf, mean
from pyspark.sql.types import DoubleType

# Ορισμός της συνάρτησης Haversine σε καθαρή Python
def calculate_haversine(lat1, lon1, lat2, lon2):
    # Έλεγχος για κενές τιμές
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
        
    R = 6371.0 # Ακτίνα της Γης σε χλμ
    
    # Μετατροπή σε ακτίνια (radians)
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c

def main():
    spark = SparkSession.builder.appName("Q2_DataFrame_UDF").getOrCreate()
    
    # Μετατροπή της συνάρτησης Python σε Spark UDF
    haversine_udf = udf(calculate_haversine, DoubleType())
    
    data_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2015.csv"
    
    start_time = time.time()
    print("\n" + "="*50)
    print("=== ΞΕΚΙΝΑΕΙ Η ΦΟΡΤΩΣΗ ΤΩΝ ΔΕΔΟΜΕΝΩΝ ΤΟΥ 2015 (UDF) ===")
    
    try:
        df = spark.read.csv(data_path, header=True, inferSchema=True)
        
        # 1. Φιλτράρισμα Έγκυρων Εγγραφών
        df_clean = df.filter(
            (col("trip_distance") > 0) & 
            (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
            (col("pickup_longitude").between(-75, -73)) & 
            (col("pickup_latitude").between(40, 42)) &
            (col("dropoff_longitude").between(-75, -73)) & 
            (col("dropoff_latitude").between(40, 42))
        )

        # 2. Υπολογισμός Διάρκειας και Ταχύτητας
        df_metrics = df_clean.withColumn(
            "duration_hours", 
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600.0
        ).withColumn(
            "speed_kmh", 
            (col("trip_distance") * 1.60934) / col("duration_hours")
        )

        # 3. Εφαρμογή του UDF για την απόσταση Haversine
        df_metrics = df_metrics.withColumn(
            "haversine_dist_km", 
            haversine_udf(col("pickup_latitude"), col("pickup_longitude"), col("dropoff_latitude"), col("dropoff_longitude"))
        )

        # 4. Detour Ratio & Outliers
        df_metrics = df_metrics.filter(col("haversine_dist_km") > 0) \
            .withColumn("detour_ratio", (col("trip_distance") * 1.60934) / col("haversine_dist_km")) \
            .filter((col("speed_kmh") > 0) & (col("speed_kmh") < 120))

        # === ΔΗΜΙΟΥΡΓΙΑ ΠΙΝΑΚΩΝ ===
        summary_by_hour = df_metrics.withColumn("hour", hour("tpep_pickup_datetime")) \
            .groupBy("hour") \
            .agg(
                round(mean("speed_kmh"), 2).alias("avg_speed_kmh"),
                round(mean("detour_ratio"), 2).alias("avg_detour_ratio")
            ).orderBy("hour")

        fastest_trips = df_metrics.select(
            "tpep_pickup_datetime", "tpep_dropoff_datetime", 
            round("trip_distance", 2).alias("dist_miles"), 
            round("speed_kmh", 2).alias("speed_kmh")
        ).orderBy(col("speed_kmh").desc()).limit(5)

        slowest_trips = df_metrics.select(
            "tpep_pickup_datetime", "tpep_dropoff_datetime", 
            round("trip_distance", 2).alias("dist_miles"), 
            round("speed_kmh", 2).alias("speed_kmh")
        ).orderBy(col("speed_kmh").asc()).limit(5)

        # Εκτύπωση αποτελεσμάτων
        print("\n=== 1. ΜΕΣΗ ΤΑΧΥΤΗΤΑ & DETOUR RATIO ΑΝΑ ΩΡΑ (UDF) ===")
        summary_by_hour.show(24)

        print("\n=== 2. ΟΙ 5 ΤΑΧΥΤΕΡΕΣ ΔΙΑΔΡΟΜΕΣ ===")
        fastest_trips.show()

        print("\n=== 3. ΟΙ 5 ΠΙΟ ΑΡΓΕΣ ΔΙΑΔΡΟΜΕΣ (ΣΥΜΦΟΡΗΣΗ) ===")
        slowest_trips.show()

    except Exception as e:
        print("\nΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΚΤΕΛΕΣΗ:", str(e))
        
    end_time = time.time()
    print("\n" + "="*50)
    print(f"Συνολικός χρόνος εκτέλεσης (UDF): {end_time - start_time} δευτερόλεπτα")
    print("="*50 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()