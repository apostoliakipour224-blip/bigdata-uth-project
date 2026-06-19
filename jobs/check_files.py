from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("CheckHDFSFiles").getOrCreate()
    sc = spark.sparkContext
    
    # Χρησιμοποιούμε τη σύνδεση του Spark (JVM) για να ρωτήσουμε το HDFS
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    
    fs = FileSystem.get(URI("hdfs://hdfs-namenode.default.svc.cluster.local:9000/"), Configuration())
    
    print("\n" + "="*50)
    print("ΤΑ ΑΡΧΕΙΑ ΜΕΣΑ ΣΤΟΝ ΦΑΚΕΛΟ /dataset/ ΕΙΝΑΙ:")
    print("-" * 50)
    
    try:
        statuses = fs.listStatus(Path("/dataset/"))
        for status in statuses:
            print(" ->", status.getPath().getName())
    except Exception as e:
        print("ΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΑΝΑΓΝΩΣΗ:", str(e))
        
    print("="*50 + "\n")
    spark.stop()

if __name__ == "__main__":
    main()