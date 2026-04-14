import pandas as pd
import numpy as np
import requests
import os

# Verified 2026 Dataset ID: Calgary Business Licenses
DATASET_ID = "vdjc-pybd"
BASE_URL = f"https://data.calgary.ca/resource/{DATASET_ID}.json"

# 20 Strategic Categories for Executive Clarity
INDUSTRY_MAP = {
    'RESTAURANT': 'Food, Beverage & Tourism', 'FOOD': 'Food, Beverage & Tourism', 'HOTEL': 'Food, Beverage & Tourism',
    'RETAIL': 'Retail & Commerce', 'DEALER': 'Retail & Commerce', 'STORE': 'Retail & Commerce',
    'PROFESSIONAL': 'Professional & Business Services', 'OFFICE': 'Professional & Business Services',
    'HEALTH': 'Health, Wellness & Personal Care', 'MEDICAL': 'Health, Wellness & Personal Care',
    'TECH': 'Tech & Innovation', 'SOFTWARE': 'Tech & Innovation', 'DATA': 'Tech & Innovation',
    'ENERGY': 'Energy & Environment', 'OIL': 'Energy & Environment', 'GAS': 'Energy & Environment',
    'CONSTRUCT': 'Construction & Real Estate', 'CONTRACTOR': 'Construction & Real Estate',
    'TRUCK': 'Logistics & Transport', 'LOGISTIC': 'Logistics & Transport',
    'MANUFACTURE': 'Industrial & Manufacturing', 'BANK': 'Finance & Legal',
    'SCHOOL': 'Education & Childcare', 'CHILD': 'Education & Childcare',
    'ART': 'Arts, Culture & Entertainment', 'FITNESS': 'Fitness & Recreation',
    'AUTO': 'Automotive Services', 'SOCIAL': 'Community & Non-Profit',
    'CLEAN': 'Residential Services', 'PET': 'Pet & Vet Services',
    'SECURITY': 'Security & Safety', 'EVENT': 'Events & Planning',
    'MAINTENANCE': 'Repair & Maintenance'
}

def categorize(val):
    val = str(val).upper()
    for k, v in INDUSTRY_MAP.items():
        if k in val: return v
    return 'Other Specialized Services'

def generate_nexus_feed():
    # Fetching only required columns to minimize payload
    params = {"$limit": 150000, "$select": "comdistnm, licencetypes"}

    try:
        print("Synchronizing Strategic Data Hub...")
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        df.columns = ['community_name', 'raw_license']

        # 1. CLEANING & AGGREGATION (Level 2: Community + Sector)
        df['industry_sector'] = df['raw_license'].apply(categorize)
        df['community_name'] = df['community_name'].str.title().str.strip()
        
        # Primary footprint count per row
        nexus = df.groupby(['community_name', 'industry_sector']).size().reset_index(name='footprint_count')

        # 2. KPI: MOMENTUM (Location Quotient)
        # Identifies if a sector is more concentrated here than the city average
        city_sector_totals = nexus.groupby('industry_sector')['footprint_count'].transform('sum')
        city_total = nexus['footprint_count'].sum()
        comm_totals = nexus.groupby('community_name')['footprint_count'].transform('sum')
        
        nexus['momentum'] = (nexus['footprint_count'] / comm_totals) / (city_sector_totals / city_total)

        # 3. KPI: RESILIENCE (Shannon-style Diversity)
        # How diverse is the community's economy?
        sector_variety = nexus.groupby('community_name')['industry_sector'].transform('count')
        nexus['resilience'] = sector_variety / 20.0  # Relative to our 20 categories

        # 4. KPI: ACCELERATION (Local Market Share)
        nexus['acceleration'] = nexus['footprint_count'] / comm_totals

        # 5. NORMALIZATION (0.0 - 1.0)
        # Scaled for consistent visual rendering in Tableau
        for m in ['footprint_count', 'resilience', 'momentum', 'acceleration']:
            mn, mx = nexus[m].min(), nexus[m].max()
            nexus[f'n_{m}'] = (nexus[m] - mn) / (mx - mn) if mx != mn else 1.0

        # 6. VITALITY INDEX (The HUD Master Score)
        # Weights: Diversity(40%), Concentration(30%), Volume(20%), Capture(10%)
        nexus['health_score'] = (nexus['n_resilience'] * 0.40 + nexus['n_momentum'] * 0.30 + 
                                 nexus['n_footprint_count'] * 0.20 + nexus['n_acceleration'] * 0.10)

        # 7. EXPORT
        os.makedirs('data', exist_ok=True)
        nexus.to_csv('data/nexus_intelligence_feed.csv', index=False)
        print(f"Deployment Ready: {len(nexus)} strategic rows generated.")

    except Exception as e:
        print(f"System Operational Failure: {e}")
        exit(1)

if __name__ == "__main__":
    generate_nexus_feed()
