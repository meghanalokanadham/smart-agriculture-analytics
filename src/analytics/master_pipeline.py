"""
Master Distributed Agricultural Analytics Execution Pipeline
Tasks executed:
1. Load 15,000 multi-source agronomic records (USDA / FAOSTAT / ISRIC Soil / NASA POWER)
2. Distributed RDD/DataFrame ETL & Data Cleansing:
   - Handle outliers, type conversions, missing values
   - Feature engineering: NPK Ratio, Moisture-to-Rainfall Index, Thermal-Radiation Index
3. Distributed Aggregations & Big Data Metrics:
   - Crop yield performance per region & climate zone
   - Soil fertility index analysis per soil type
   - Irrigation efficiency vs yield impact
4. Unsupervised ML: Distributed K-Means Clustering on Soil & Weather Dynamics
   - Cluster profiling and naming (e.g. Drought-Prone High NPK, Humid Optimal Loam, etc.)
5. Supervised ML: Gaussian Naive Bayes Classification for Yield Efficiency (High/Medium/Low)
   - Train/Test Split (80/20)
   - Accuracy, Precision, Recall, Confusion Matrix
6. Export structured JSON for interactive visual dashboards
"""

import os
import sys
import json
import time

# Add current project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.engine.distributed_framework import DistDataFrame, DistRDD
from src.ml.ml_pipeline import StandardScaler, DistributedKMeans, GaussianNBClassifier, ModelEvaluator

def run_pipeline():
    start_time = time.time()
    print("=" * 80)
    print("   BIG DATA DISTRIBUTED SMART AGRICULTURE ANALYTICS PLATFORM (V2.5)   ")
    print("=" * 80)

    raw_path = "data/raw/agri_sensor_yield_dataset.csv"
    processed_path = "data/processed/agri_analytics_master.csv"
    report_json_path = "reports/analytics_results.json"

    # Step 1: Distributed Load & Ingestion
    print("\n[STEP 1] Ingesting multi-source agricultural datasets into Distributed DataFrame...")
    df = DistDataFrame.read_csv(raw_path, num_partitions=6)
    initial_count = df.count()
    print(f"-> Successfully loaded {initial_count:,} records partitioned across 6 virtual worker nodes.")
    print(f"-> Schema: {df.columns()}")

    # Step 2: Feature Engineering & Agronomic Index Calculation
    print("\n[STEP 2] Distributed Transformations & Feature Engineering...")
    # NPK Ratio calculation: Nitrogen / (Phosphorus + Potassium)
    df_engineered = df.withColumn(
        "npk_balance_ratio",
        lambda r: round(float(r["nitrogen_ppm"]) / max(1.0, float(r["phosphorus_ppm"]) + float(r["potassium_ppm"])), 3)
    ).withColumn(
        "thermal_stress_index",
        lambda r: round(abs(float(r["avg_temperature_c"]) - 22.5) * (1.0 + float(r["solar_radiation_mj"]) / 25.0), 2)
    ).withColumn(
        "soil_fertility_score",
        lambda r: round(
            (float(r["organic_matter_pct"]) * 10.0) +
            (float(r["nitrogen_ppm"]) * 0.15) +
            (float(r["phosphorus_ppm"]) * 0.3) +
            (float(r["potassium_ppm"]) * 0.1) -
            (abs(float(r["soil_ph"]) - 6.5) * 8.0), 2
        )
    )
    print("-> Engineered features: npk_balance_ratio, thermal_stress_index, soil_fertility_score")

    # Step 3: Distributed Analytical Aggregations
    print("\n[STEP 3] Performing Catalyst-Style Distributed Aggregations...")
    print("\n--- [A] Regional Agronomic Benchmark (Mean Yield, Rainfall, Soil Fertility) ---")
    reg_summary = df_engineered.groupBy("region").agg({
        "crop_yield_ton_ha": ["avg", "max", "std"],
        "annual_rainfall_mm": ["avg"],
        "soil_fertility_score": ["avg"]
    }).orderBy("avg_crop_yield_ton_ha", ascending=False)
    reg_summary.show(10)

    print("\n--- [B] Crop-Specific Performance Across Soil Types ---")
    crop_soil_summary = df_engineered.groupBy("crop_type", "soil_type").agg({
        "crop_yield_ton_ha": ["avg"],
        "soil_fertility_score": ["avg"],
        "organic_matter_pct": ["avg"]
    }).orderBy("avg_crop_yield_ton_ha", ascending=False)
    crop_soil_summary.show(8)

    print("\n--- [C] Irrigation System Efficiency Comparison ---")
    irrig_summary = df_engineered.groupBy("irrigation_type").agg({
        "crop_yield_ton_ha": ["avg"],
        "soil_moisture_pct": ["avg"],
        "fertilizer_usage_kg_ha": ["avg"]
    }).orderBy("avg_crop_yield_ton_ha", ascending=False)
    irrig_summary.show(10)

    # Step 4: Machine Learning - Unsupervised Distributed K-Means Clustering
    print("\n[STEP 4] Executing Distributed K-Means Clustering for Soil-Climatic Zonation...")
    all_data = df_engineered.collect()
    ml_features = ["soil_ph", "organic_matter_pct", "nitrogen_ppm", "phosphorus_ppm", "potassium_ppm", "annual_rainfall_mm", "avg_temperature_c"]
    
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(all_data, ml_features)
    
    kmeans = DistributedKMeans(k=4, max_iter=30, seed=42)
    kmeans.fit(scaled_data, [f"scaled_{f}" for f in ml_features])
    cluster_labels = kmeans.predict(scaled_data)

    cluster_names = {
        0: "Zone Alpha: High-Rainfall Acidic Subtropical (High Leaching)",
        1: "Zone Beta: Optimal Loamy Balanced Temperate (High Yield Potential)",
        2: "Zone Gamma: Semi-Arid Alkaline Low-Precipitation (Drought Prone)",
        3: "Zone Delta: Tropical High-Nutrient High-Temperature"
    }

    for idx, r in enumerate(all_data):
        r["agro_climatic_cluster"] = cluster_labels[idx]
        r["cluster_designation"] = cluster_names[cluster_labels[idx]]

    print(f"-> K-Means Converged in {kmeans.k} clusters. Total Inertia (WCSS): {kmeans.inertia_:.2f}")
    
    # Cluster profile counts
    cluster_counts = {}
    for c in cluster_labels:
        cluster_counts[c] = cluster_counts.get(c, 0) + 1
    for c_id, count in sorted(cluster_counts.items()):
        print(f"   * Cluster {c_id} ({cluster_names[c_id]}): {count:,} samples ({count/len(all_data)*100:.1f}%)")

    # Step 5: Supervised Classification - Crop Yield Efficiency Predictor
    print("\n[STEP 5] Training Supervised Classification Model for Yield Efficiency...")
    # Train / Test split (80% train, 20% test)
    split_idx = int(0.8 * len(all_data))
    train_data = all_data[:split_idx]
    test_data = all_data[split_idx:]
    print(f"-> Dataset partition: {len(train_data):,} training samples, {len(test_data):,} testing samples.")

    classifier_features = [
        "soil_ph", "organic_matter_pct", "nitrogen_ppm", "phosphorus_ppm", "potassium_ppm",
        "soil_moisture_pct", "annual_rainfall_mm", "avg_temperature_c", "solar_radiation_mj",
        "fertilizer_usage_kg_ha", "pest_pressure_score", "soil_fertility_score"
    ]
    target_col = "yield_efficiency_category"

    clf = GaussianNBClassifier()
    clf.fit(train_data, classifier_features, target_col)
    predictions = clf.predict(test_data)
    actuals = [r[target_col] for r in test_data]

    eval_results = ModelEvaluator.classification_metrics(actuals, predictions, clf.classes)
    print(f"\n-> Supervised Model Accuracy: {eval_results['accuracy'] * 100:.2f}%")
    print(f"-> Macro F1-Score: {eval_results['macro_f1']:.4f}")
    print("\n--- Per-Class Performance Metrics ---")
    for cls_name, met in eval_results["per_class"].items():
        print(f"   * Class '{cls_name}': Precision={met['precision']:.3f}, Recall={met['recall']:.3f}, F1={met['f1_score']:.3f}, Support={met['support']}")

    print("\n--- Confusion Matrix ---")
    header_str = "Actual \\ Pred | " + " | ".join([f"{c:>8}" for c in clf.classes])
    print(header_str)
    print("-" * len(header_str))
    for actual_c in clf.classes:
        row_str = f"{actual_c:<13} | " + " | ".join([f"{eval_results['confusion_matrix'][actual_c][pred_c]:>8}" for pred_c in clf.classes])
        print(row_str)

    # Step 6: Export Processed Master Data & Results
    print("\n[STEP 6] Exporting Processed Big Data & Analytical JSON...")
    df_final = DistDataFrame(all_data, schema=list(all_data[0].keys()))
    df_final.to_csv(processed_path)
    print(f"-> Processed dataset saved to: {processed_path}")

    # Build detailed summary JSON for UI Dashboard
    dashboard_payload = {
        "execution_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_records_processed": initial_count,
        "execution_duration_sec": round(time.time() - start_time, 2),
        "cluster_metrics": {
            "k": kmeans.k,
            "inertia": round(kmeans.inertia_, 2),
            "clusters": [
                {"id": c_id, "name": cluster_names[c_id], "sample_count": cluster_counts[c_id]}
                for c_id in sorted(cluster_counts.keys())
            ]
        },
        "classification_metrics": {
            "accuracy": eval_results["accuracy"],
            "macro_f1": eval_results["macro_f1"],
            "per_class": eval_results["per_class"],
            "confusion_matrix": eval_results["confusion_matrix"]
        },
        "regional_benchmarks": reg_summary.collect(),
        "irrigation_benchmarks": irrig_summary.collect(),
        "crop_soil_benchmarks": crop_soil_summary.collect()[:15],
        "top_features": classifier_features
    }

    os.makedirs(os.path.dirname(report_json_path), exist_ok=True)
    with open(report_json_path, mode="w", encoding="utf-8") as f:
        json.dump(dashboard_payload, f, indent=2)
    print(f"-> Analytics summary generated at: {report_json_path}")
    print("\n" + "=" * 80)
    print(f"   PIPELINE EXECUTION FINISHED SUCCESSFULLY IN {time.time() - start_time:.2f}s   ")
    print("=" * 80)

if __name__ == "__main__":
    run_pipeline()
