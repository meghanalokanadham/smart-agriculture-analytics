from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, round as spark_round, when, abs as spark_abs
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main():
    print(">>> Initializing Apache Spark Distributed Session for Smart Agriculture Platform...")
    spark = SparkSession.builder \
        .appName("SmartAgricultureBigDataAnalytics") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Ingestion
    dataset_path = "data/raw/agri_sensor_yield_dataset.csv"
    print(f">>> Reading raw telemetry and soil data from {dataset_path}...")
    df = spark.read.csv(dataset_path, header=True, inferSchema=True)
    df.printSchema()

    # Feature Engineering
    print(">>> Performing Catalyst transformations & Agronomic Indices...")
    df_transformed = df.withColumn(
        "npk_balance_ratio",
        spark_round(col("nitrogen_ppm") / (col("phosphorus_ppm") + col("potassium_ppm")), 3)
    ).withColumn(
        "soil_fertility_score",
        spark_round((col("organic_matter_pct") * 10.0) + (col("nitrogen_ppm") * 0.15) + (col("phosphorus_ppm") * 0.3) - (spark_abs(col("soil_ph") - 6.5) * 8.0), 2)
    )

    # Distributed Aggregations
    print(">>> Executing distributed aggregations by region...")
    reg_summary = df_transformed.groupBy("region").agg(
        spark_round(avg("crop_yield_ton_ha"), 2).alias("avg_yield"),
        spark_round(avg("annual_rainfall_mm"), 1).alias("avg_rainfall"),
        spark_round(avg("soil_fertility_score"), 1).alias("avg_fertility")
    ).orderBy(col("avg_yield").desc())
    reg_summary.show(10, truncate=False)

    # Machine Learning - Vector Assembler
    feature_cols = ["soil_ph", "organic_matter_pct", "nitrogen_ppm", "phosphorus_ppm", "potassium_ppm", "annual_rainfall_mm", "avg_temperature_c"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    assembled_data = assembler.transform(df_transformed)

    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    scaled_data = scaler.fit(assembled_data).transform(assembled_data)

    # K-Means Clustering
    print(">>> Fitting Spark MLlib Distributed KMeans (k=4)...")
    kmeans = KMeans(k=4, seed=42, featuresCol="features", predictionCol="cluster_id")
    km_model = kmeans.fit(scaled_data)
    clustered_df = km_model.transform(scaled_data)
    clustered_df.groupBy("cluster_id").count().show()

    print(">>> Pipeline completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
