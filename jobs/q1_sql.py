from pyspark.sql import SparkSession
import time

# 1. Αρχικοποίηση
spark = SparkSession.builder.appName("Taxi-Q1-SQL").getOrCreate()
start_time = time.time()

# 2. Φόρτωση των δεδομένων από το Parquet
parquet_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/akipouridou/project2026/data/parquet/yellow_tripdata_2024"
df = spark.read.parquet(parquet_path)

# 3. Το "Μυστικό" του Spark SQL: Δημιουργία View
# Αυτή η εντολή παίρνει το DataFrame και το κάνει να συμπεριφέρεται σαν 
# κανονικός πίνακας βάσης δεδομένων με το όνομα "trips_2024"
df.createOrReplaceTempView("trips_2024")

# 4. Το ερώτημα SQL
# Εδώ γράφουμε καθαρή SQL. Ζητάμε τα ίδια ακριβώς πράγματα με το Q1 DF, 
# για το δικό σου παράθυρο ωρών (03:00 - 06:59).
query = """
SELECT 
    pickup_day AS pickup_date,
    pickup_hour,
    COUNT(*) AS trips,
    COUNT(DISTINCT pulocationid) AS unique_pickup_zones,
    AVG(passenger_count) AS avg_passenger_count,
    AVG(trip_duration_minutes) AS avg_duration_minutes,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(total_amount) AS avg_total_amount,
    SUM(total_amount) AS total_revenue,
    (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()) AS trip_share_in_personal_window
FROM trips_2024
WHERE pickup_hour BETWEEN 3 AND 6
GROUP BY pickup_day, pickup_hour
ORDER BY trips DESC
LIMIT 12
"""

print("--- Top 12 Ώρες/Ημέρες Ζήτησης (Spark SQL API) ---")

# 5. Εκτέλεση και Εμφάνιση
sql_results = spark.sql(query)
sql_results.show(truncate=False)

# 6. Κλείσιμο και χρόνος
end_time = time.time()
print(f"SQL Query Execution Time: {end_time - start_time:.2f} seconds")
spark.stop()