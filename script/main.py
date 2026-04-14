import pandas as pd
import numpy as np
import requests
import os

# CENTRALIZED STRATEGIC MAPPING
# Maps raw Calgary license types to ~30 strategic categories
INDUSTRY_MAP = {
    'RESTAURANT': 'Food & Beverage', 'FOOD': 'Food & Beverage', 'LIQUOR': 'Food & Beverage',
    'RETAIL': 'Retail Commerce', 'DEALER': 'Retail Commerce', 'STORE': 'Retail Commerce',
    'PROFESSIONAL': 'Professional Services', 'OFFICE': 'Professional Services', 'CONSULT': 'Professional Services',
    'HEALTH': 'Health & Wellness', 'MEDICAL': 'Health & Wellness', 'CLINIC': 'Health & Wellness',
    'TECH': 'Technology & Innovation', 'SOFTWARE': 'Technology & Innovation', 'DATA': 'Technology & Innovation',
    'CONSTRUCT': 'Construction & Reno', 'CONTRACTOR': 'Construction & Reno', 'BUILD': 'Construction & Reno',
    'AUTO': 'Automotive Services', 'VEHICLE': 'Automotive Services', 'REPAIR': 'Automotive Services',
    'SALON': 'Personal Care', 'SPA': 'Personal Care', 'BEAUTY': 'Personal Care',
    'TRUCK': 'Logistics & Transport', 'LOGISTIC': 'Logistics & Transport', 'WAREHOUSE': 'Logistics & Transport',
    'SCHOOL': 'Education & Training', 'INSTRUCTION': 'Education & Training',
    'BANK': 'Financial Services', 'FINANCE': 'Financial Services', 'INSURANCE': 'Financial Services',
    'OIL': 'Energy & Utilities', 'GAS': 'Energy & Utilities', 'POWER': 'Energy & Utilities',
    'CLEAN': 'Residential Services', 'LANDSCAPE': 'Residential Services',
    'FITNESS': 'Fitness & Sports', 'GYM': 'Fitness & Sports', 'RECREATION': 'Fitness & Sports',
    'HOTEL': 'Accommodation', 'MOTEL': 'Accommodation', 'LODGING': 'Accommodation',
    'PET': 'Pet Services', 'VET': 'Pet Services', 'ANIMAL': 'Pet Services',
    'REAL ESTATE': 'Real Estate', 'PROPERTY': 'Real Estate', 'LEASING': 'Real Estate',
    'MEDIA': 'Media & Marketing', 'DESIGN': 'Media & Marketing', 'ADVERT': 'Media & Marketing',
    'SOCIAL': 'Community & NGO', 'NON-PROFIT': 'Community & NGO', 'CHARITY': 'Community & NGO',
    'SECURITY': 'Security Services', 'ALARM': 'Security Services',
    'WASTE': 'Environmental Services', 'RECYCLE': 'Environmental Services',
    'ENTERTAIN': 'Entertainment', 'CINEMA': 'Entertainment', 'THEATRE': 'Entertainment',
    'TOUR': 'Tourism', 'TRAVEL': 'Tourism',
    'MAINTENANCE': 'General Maintenance', 'TOOL': 'General Maintenance',
    'CHILD': 'Childcare', 'DAYCARE': 'Childcare',
    'LAUNDRY': 'Laundry Services', 'CLEANING': 'Laundry Services',
    'EVENT': 'Events & Planning', 'WEDDING': 'Events & Planning',
    'MANUFACTURE': 'Manufacturing', 'FABRICATION': 'Manufacturing'
}

def optimized_categorize(raw_val):
    raw_val = str(raw_val).upper()
    for key, category in INDUSTRY_MAP.items():
        if key in raw_val:
            return category
    return 'Specialized/Other'

def generate_nexus_feed():
    DATASET_URL = "https://data.calgary.ca/resource/be6q-p884.json"
    params = {
        "$limit": 100000,
        "$select": "communityname, licensetypes, count(licensetypes)",
        "$where": "communityname IS NOT NULL AND licensetypes IS NOT NULL",
        "$group": "communityname, licensetypes"
    }

    try:
        print("Connecting to Calgary Open Data Portal...")
        response = requests.get(DATASET_URL, params=params, timeout=30)
        response.raise_for_status()
        
        df = pd.DataFrame(response.json())
        df.columns = ['community_name', 'raw_license', 'footprint_count']
        df['footprint_count'] = pd.to_numeric(df['footprint_count'])

        # 1. Categorization & Consolidation
        df['industry_sector'] = df['raw_license'].apply(optimized_categorize)
        df = df.groupby(['community_name', 'industry_sector'], as_index=False)['footprint_count'].sum()

        # 2. Hygiene & Resilience KPI
        df['community_name'] = df['community_name'].str.title().str.strip()
        df['resilience'] = df.groupby('community_name')['industry_sector'].transform('count')
        
        # 3. Simulated Momentum/Acceleration
        state = np.random.RandomState(42)
        df['momentum'] = state.uniform(0.1, 0.9, size=len(df))
        df['acceleration'] = state.uniform(-0.1, 0.3, size=len(df))

        # 4. Normalization (0.0 - 1.0)
        for m in ['footprint_count', 'resilience', 'momentum', 'acceleration']:
            c_min, c_max = df[m].min(), df[m].max()
            df[f'n_{m}'] = (df[m] - c_min) / (c_max - c_min) if c_max != c_min else 1.0

        # 5. Vitality Index
        df['health_score'] = (df['n_resilience'] * 0.35 + df['n_footprint_count'] * 0.35 + 
                              df['n_momentum'] * 0.15 + df['n_acceleration'] * 0.15)

        # 6. Export to Root-level Data Folder
        # Note: 'data' folder is handled by the GitHub Action
        output_path = 'data/nexus_intelligence_feed.csv'
        df.to_csv(output_path, index=False)
        print(f"Success: Exported {len(df)} rows to {output_path}")

    except Exception as e:
        print(f"Pipeline Error: {e}")
        exit(1)

if __name__ == "__main__":
    generate_nexus_feed()
