import pandas as pd
import numpy as np
import requests
import os

# Official 2026 Dataset ID: 'vdjc-pybd' (Calgary Business Licenses)
DATASET_ID = "vdjc-pybd"
BASE_URL = f"https://data.calgary.ca/resource/{DATASET_ID}.json"

# STRATEGIC MAPPING: Consolidated to ~30 Industry Sectors
# Logic: Key (Raw keyword) -> Value (Strategic Category)
INDUSTRY_MAP = {
    'RESTAURANT': 'Food & Beverage', 'FOOD': 'Food & Beverage', 'LIQUOR': 'Food & Beverage',
    'RETAIL': 'Retail Commerce', 'DEALER': 'Retail Commerce', 'STORE': 'Retail Commerce',
    'PROFESSIONAL': 'Professional Services', 'OFFICE': 'Professional Services',
    'HEALTH': 'Health & Wellness', 'MEDICAL': 'Health & Wellness', 'CLINIC': 'Health & Wellness',
    'TECH': 'Technology & Innovation', 'SOFTWARE': 'Technology & Innovation',
    'CONSTRUCT': 'Construction & Reno', 'CONTRACTOR': 'Construction & Reno',
    'AUTO': 'Automotive Services', 'VEHICLE': 'Automotive Services', 'REPAIR': 'Automotive Services',
    'SALON': 'Personal Care', 'SPA': 'Personal Care', 'BEAUTY': 'Personal Care',
    'TRUCK': 'Logistics & Transport', 'LOGISTIC': 'Logistics & Transport',
    'SCHOOL': 'Education & Training', 'BANK': 'Financial Services', 'FINANCE': 'Financial Services',
    'OIL': 'Energy & Utilities', 'GAS': 'Energy & Utilities', 'POWER': 'Energy & Utilities',
    'CLEAN': 'Residential Services', 'LANDSCAPE': 'Residential Services',
    'FITNESS': 'Fitness & Sports', 'GYM': 'Fitness & Sports',
    'HOTEL': 'Accommodation', 'PET': 'Pet Services', 'VET': 'Pet Services',
    'REAL ESTATE': 'Real Estate', 'MEDIA': 'Media & Marketing',
    'SOCIAL': 'Community & NGO', 'SECURITY': 'Security Services',
    'WASTE': 'Environmental Services', 'ENTERTAIN': 'Entertainment',
    'TOUR': 'Tourism', 'CHILD': 'Childcare', 'MANUFACTURE': 'Manufacturing'
}

def categorize_sector(raw_val):
    """High-speed string matching for industry aggregation"""
    raw_val = str(raw_val).upper()
    for key, category in INDUSTRY_MAP.items():
        if key in raw_val:
            return category
    return 'Specialized/Other'

def generate_nexus_feed():
    # 2026 Schema Field Names: 'comdistnm' (Community), 'licencetypes' (License)
    params = {
        "$limit": 100000,
        "$select": "comdistnm, licencetypes, count(licencetypes)",
        "$where": "comdistnm IS NOT NULL AND licencetypes IS NOT NULL",
        "$group": "comdistnm, licencetypes"
    }

    try:
        print(f"Initializing Calgary Nexus Gateway (Endpoint: {DATASET_ID})...")
        response = requests.get(BASE_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"Connection Failed: {response.status_code}. URL: {response.url}")
            response.raise_for_status()
        
        df = pd.DataFrame(response.json())
        df.columns = ['community_name', 'raw_license', 'footprint_count']
        df['footprint_count'] = pd.to_numeric(df['footprint_count'])

        # 1. TRANSFORM: Categorize and Re-aggregate
        df['industry_sector'] = df['raw_license'].apply(categorize_sector)
        df = df.groupby(['community_name', 'industry_sector'], as_index=False)['footprint_count'].sum()

        # 2. HYGIENE: Standardize Names for Tableau
        df['community_name'] = df['community_name'].str.title().str.strip()
        
        # 3. KPI LOGIC: Resilience (Diversity)
        df['resilience'] = df.groupby('community_name')['industry_sector'].transform('count')
        
        # Simulated Real-Time Layers (Replace with YoY diff for live growth)
        state = np.random.RandomState(42)
        df['momentum'] = state.uniform(0.1, 0.9, size=len(df))
        df['acceleration'] = state.uniform(-0.1, 0.3, size=len(df))

        # 4. NORMALIZATION (0.0 - 1.0)
        for m in ['footprint_count', 'resilience', 'momentum', 'acceleration']:
            c_min, c_max = df[m].min(), df[m].max()
            df[f'n_{m}'] = (df[m] - c_min) / (c_max - c_min) if c_max != c_min else 1.0

        # 5. VITALITY INDEX (HUD Master Score)
        df['health_score'] = (df['n_resilience'] * 0.35 + df['n_footprint_count'] * 0.35 + 
                              df['n_momentum'] * 0.15 + df['n_acceleration'] * 0.15)

        # 6. EXPORT
        os.makedirs('data', exist_ok=True)
        df.to_csv('data/nexus_intelligence_feed.csv', index=False)
        print("Success: Nexus Intelligence Feed updated.")

    except Exception as e:
        print(f"Operational Failure: {e}")
        exit(1)

if __name__ == "__main__":
    generate_nexus_feed()
