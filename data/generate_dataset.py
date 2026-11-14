"""
Smart Agriculture Data Generator & Ingestion Pipeline
Source Modeling:
- USDA NASS (National Agricultural Statistics Service) & FAOSTAT Agronomic Profiles
- ISRIC SoilGrids Chemical & Physical Parameters (pH, Organic Matter %, Nitrogen, Phosphorus, Potassium, Moisture)
- NASA POWER Agro-Climatology Satellite Precipitation, Temperature, Solar Radiation
- Multi-region agro-ecological zones: Midwest Corn Belt, Indo-Gangetic Plains, Mediterranean Basin, Sub-Saharan Savanna, Brazilian Cerrado
"""

import os
import csv
import json
import math
import random
import time

REGIONS = {
    "Midwest_Corn_Belt_US": {
        "climate": "Temperate_Continental",
        "crops": ["Corn", "Soybean", "Wheat"],
        "temp_mean": 18.5, "temp_std": 6.2,
        "rainfall_mean": 950.0, "rainfall_std": 180.0,
        "ph_mean": 6.4, "ph_std": 0.4,
        "n_mean": 140.0, "p_mean": 55.0, "k_mean": 180.0,
        "organic_matter_mean": 3.8
    },
    "Indo_Gangetic_Plain_IN": {
        "climate": "Subtropical_Monsoonal",
        "crops": ["Rice", "Wheat", "Sugarcane", "Mustard"],
        "temp_mean": 26.0, "temp_std": 5.5,
        "rainfall_mean": 1100.0, "rainfall_std": 250.0,
        "ph_mean": 7.3, "ph_std": 0.5,
        "n_mean": 110.0, "p_mean": 42.0, "k_mean": 160.0,
        "organic_matter_mean": 1.2
    },
    "Cerrado_Central_BR": {
        "climate": "Tropical_Savanna",
        "crops": ["Soybean", "Corn", "Cotton"],
        "temp_mean": 24.5, "temp_std": 3.2,
        "rainfall_mean": 1400.0, "rainfall_std": 220.0,
        "ph_mean": 5.2, "ph_std": 0.6,
        "n_mean": 95.0, "p_mean": 30.0, "k_mean": 120.0,
        "organic_matter_mean": 2.4
    },
    "Mediterranean_Basin_ES": {
        "climate": "Dry_Summer_Subtropical",
        "crops": ["Barley", "Olive", "Wheat", "Grape"],
        "temp_mean": 21.0, "temp_std": 4.8,
        "rainfall_mean": 520.0, "rainfall_std": 110.0,
        "ph_mean": 7.8, "ph_std": 0.4,
        "n_mean": 75.0, "p_mean": 28.0, "k_mean": 190.0,
        "organic_matter_mean": 1.6
    },
    "East_African_Rift_KE": {
        "climate": "Tropical_Highland",
        "crops": ["Maize", "Coffee", "Tea", "Cassava"],
        "temp_mean": 20.0, "temp_std": 3.5,
        "rainfall_mean": 880.0, "rainfall_std": 190.0,
        "ph_mean": 5.9, "ph_std": 0.5,
        "n_mean": 80.0, "p_mean": 35.0, "k_mean": 140.0,
        "organic_matter_mean": 3.0
    }
}

CROP_PROFILES = {
    "Corn": {"opt_ph": (5.8, 7.0), "opt_rain": (600, 1100), "opt_temp": (18, 28), "base_yield": 9.5, "yield_var": 2.2},
    "Maize": {"opt_ph": (5.8, 7.0), "opt_rain": (600, 1000), "opt_temp": (18, 27), "base_yield": 4.2, "yield_var": 1.1},
    "Soybean": {"opt_ph": (6.0, 7.0), "opt_rain": (550, 950), "opt_temp": (20, 30), "base_yield": 3.4, "yield_var": 0.8},
    "Wheat": {"opt_ph": (6.0, 7.5), "opt_rain": (450, 850), "opt_temp": (14, 23), "base_yield": 4.8, "yield_var": 1.2},
    "Rice": {"opt_ph": (5.5, 6.8), "opt_rain": (1000, 1800), "opt_temp": (22, 32), "base_yield": 5.6, "yield_var": 1.4},
    "Sugarcane": {"opt_ph": (6.0, 7.8), "opt_rain": (1100, 2000), "opt_temp": (24, 34), "base_yield": 72.0, "yield_var": 12.0},
    "Cotton": {"opt_ph": (5.8, 7.5), "opt_rain": (600, 1100), "opt_temp": (22, 32), "base_yield": 2.8, "yield_var": 0.7},
    "Barley": {"opt_ph": (6.5, 8.0), "opt_rain": (380, 700), "opt_temp": (12, 22), "base_yield": 3.9, "yield_var": 0.9},
    "Olive": {"opt_ph": (6.5, 8.2), "opt_rain": (400, 750), "opt_temp": (16, 26), "base_yield": 5.2, "yield_var": 1.0},
    "Coffee": {"opt_ph": (5.2, 6.3), "opt_rain": (1000, 1600), "opt_temp": (18, 25), "base_yield": 2.1, "yield_var": 0.5},
    "Tea": {"opt_ph": (4.5, 5.8), "opt_rain": (1200, 2200), "opt_temp": (16, 24), "base_yield": 2.5, "yield_var": 0.6},
    "Cassava": {"opt_ph": (5.0, 7.0), "opt_rain": (700, 1400), "opt_temp": (23, 32), "base_yield": 14.5, "yield_var": 3.2},
    "Mustard": {"opt_ph": (6.0, 7.5), "opt_rain": (350, 650), "opt_temp": (15, 24), "base_yield": 1.8, "yield_var": 0.4},
    "Grape": {"opt_ph": (6.2, 7.8), "opt_rain": (450, 750), "opt_temp": (16, 26), "base_yield": 9.0, "yield_var": 2.0}
}

def generate_agricultural_dataset(num_records=15000, seed=42, output_csv="data/raw/agri_sensor_yield_dataset.csv"):
    random.seed(seed)
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    
    headers = [
        "record_id", "region", "climate_zone", "crop_type", "soil_type",
        "soil_ph", "organic_matter_pct", "nitrogen_ppm", "phosphorus_ppm", "potassium_ppm",
        "soil_moisture_pct", "annual_rainfall_mm", "avg_temperature_c", "solar_radiation_mj",
        "irrigation_type", "fertilizer_usage_kg_ha", "pest_pressure_score",
        "crop_yield_ton_ha", "yield_efficiency_category"
    ]
    
    soil_types = ["Loam", "Clay_Loam", "Sandy_Loam", "Silt_Loam", "Black_Soil", "Alluvial", "Red_Sandy"]
    irrigation_types = ["Drip", "Sprinkler", "Flood_Furrow", "Rainfed"]
    
    records = []
    print(f"[*] Synthesizing {num_records} robust agronomic records modeled on USDA-NASS & FAOSTAT standards...")
    
    for i in range(1, num_records + 1):
        rec_id = f"AGRI-{i:06d}"
        region_name, reg_cfg = random.choice(list(REGIONS.items()))
        climate = reg_cfg["climate"]
        crop = random.choice(reg_cfg["crops"])
        soil = random.choice(soil_types)
        
        # Soil parameters
        ph = round(max(3.8, min(9.2, random.gauss(reg_cfg["ph_mean"], reg_cfg["ph_std"]))), 2)
        om = round(max(0.3, min(8.5, random.gauss(reg_cfg["organic_matter_mean"], 0.6))), 2)
        n = round(max(15.0, random.gauss(reg_cfg["n_mean"], 25.0)), 1)
        p = round(max(5.0, random.gauss(reg_cfg["p_mean"], 10.0)), 1)
        k = round(max(30.0, random.gauss(reg_cfg["k_mean"], 35.0)), 1)
        
        # Weather
        rainfall = round(max(150.0, random.gauss(reg_cfg["rainfall_mean"], reg_cfg["rainfall_std"])), 1)
        temp = round(max(5.0, min(42.0, random.gauss(reg_cfg["temp_mean"], reg_cfg["temp_std"]))), 1)
        solar = round(max(10.0, min(28.0, 16.0 + 0.3 * temp + random.uniform(-2.5, 2.5))), 1)
        
        # Soil moisture is correlated with rainfall and irrigation
        irrigation = random.choice(irrigation_types)
        irrigation_boost = 12.0 if irrigation == "Drip" else (8.0 if irrigation == "Sprinkler" else (15.0 if irrigation == "Flood_Furrow" else 0.0))
        moisture = round(max(8.0, min(55.0, (rainfall / 45.0) + irrigation_boost + random.gauss(0, 3.0))), 1)
        
        # Management
        fert_base = (n + p + k) * 0.45 + random.uniform(-20, 20)
        fertilizer_kg = round(max(30.0, min(400.0, fert_base)), 1)
        pest_pressure = round(max(0.05, min(0.95, random.betavariate(2, 5))), 2)
        
        # Scientific Yield Calculation Model
        c_prof = CROP_PROFILES[crop]
        opt_ph_min, opt_ph_max = c_prof["opt_ph"]
        opt_rain_min, opt_rain_max = c_prof["opt_rain"]
        opt_temp_min, opt_temp_max = c_prof["opt_temp"]
        
        # Penalty multipliers
        ph_factor = 1.0 - (0.25 * abs(ph - (opt_ph_min + opt_ph_max)/2.0))
        rain_factor = 1.0 if (opt_rain_min <= rainfall <= opt_rain_max) else max(0.4, 1.0 - 0.0008 * min(abs(rainfall - opt_rain_min), abs(rainfall - opt_rain_max)))
        temp_factor = 1.0 if (opt_temp_min <= temp <= opt_temp_max) else max(0.5, 1.0 - 0.04 * min(abs(temp - opt_temp_min), abs(temp - opt_temp_max)))
        fert_factor = min(1.2, 0.7 + (n + p*1.5 + k*0.8) / 350.0)
        pest_penalty = 1.0 - (pest_pressure * 0.35)
        
        overall_health = max(0.3, ph_factor * rain_factor * temp_factor * fert_factor * pest_penalty)
        calc_yield = c_prof["base_yield"] * overall_health + random.gauss(0, c_prof["yield_var"] * 0.2)
        crop_yield = round(max(0.2, calc_yield), 2)
        
        # Yield Efficiency Classification (High, Medium, Low)
        rel_efficiency = calc_yield / c_prof["base_yield"]
        if rel_efficiency >= 1.05:
            efficiency_cat = "High"
        elif rel_efficiency >= 0.75:
            efficiency_cat = "Medium"
        else:
            efficiency_cat = "Low"
            
        record = [
            rec_id, region_name, climate, crop, soil,
            ph, om, n, p, k, moisture, rainfall, temp, solar,
            irrigation, fertilizer_kg, pest_pressure,
            crop_yield, efficiency_cat
        ]
        records.append(record)
        
    with open(output_csv, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(records)
        
    print(f"[✓] Ingestion complete. Output generated at: {output_csv} ({len(records)} records)")
    return output_csv

if __name__ == "__main__":
    generate_agricultural_dataset(num_records=15000, seed=42)
