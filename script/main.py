import pandas as pd
import os
import requests
from datetime import datetime, timedelta

# --- BUSINESS LOGIC CONFIG ---
SECTOR_MAPPING = {
    'Energy & Resources': ['OIL', 'GAS', 'ENERGY', 'MINING', 'PETROLEUM'],
    'Hospitality & Tourism': ['RESTAURANT', 'FOOD', 'HOTEL', 'PUB', 'CATER', 'ALCOHOL'],
    'Construction & Infra': ['CONSTRUCT', 'BUILD', 'CONTRACTOR', 'PLUMB', 'ELECTRIC'],
    'Retail Trade': ['RETAIL', 'DEALER', 'STORE', 'SHOP', 'SALES'],
    'Finance & Insurance': ['FINANCE', 'BANK', 'INSURANCE', 'INVEST', 'MORTGAGE'],
    'Professional Services': ['CONSULT', 'LEGAL', 'ACCOUNT', 'ENGINEER', 'ARCHITECT'],
    'Tech & Innovation': ['SOFTWARE', 'TECH', 'DATA', 'SYSTEM', 'COMPUTER'],
    'Healthcare & Wellness': ['HEALTH', 'MEDICAL', 'DENTAL', 'CLINIC', 'HOSPITAL'],
    'Logistics & Transport': ['WAREHOUSE', 'TRUCK', 'TRANSPORT', 'LOGISTIC'],
    'Industrial & Mfg': ['MANUFACTUR', 'FACTORY', 'INDUSTRIAL', 'MACHINE'],
    'Real Estate': ['REAL ESTATE', 'PROPERTY', 'LEASING', 'RENTAL'],
    'Automotive': ['AUTO', 'VEHICLE', 'REPAIR', 'CAR WASH', 'TIRE'],
    'Agri & Environment': ['AGRI', 'FARM', 'GARDEN', 'ENVIRONMENT', 'WASTE']
}

def fetch_calgary_data():
    """Fetches data with dynamic schema discovery to prevent 400 Bad Request errors."""
    resource_id = "vdjc-pybd" 
    base_url = f"https://data.calgary.ca/resource/{resource_id}.json"
    headers = {'User-Agent': 'NexusStrategicIntelligence/2.2'}
    
    print(f"System: Identifying schema for {resource_id}...")
    
    # Discovery Step: Fetch 1 row to find the date column name
    schema_resp = requests.get(f"{base_url}?$limit=1", headers=headers, timeout=20)
    schema_resp.raise_for_status()
    sample = schema_resp.json()[0]
    
    # Find the most likely date column
    date_candidates = ['issueddate', 'issued_date', 'date_issued', 'licence_date']
    date_col = next((c for c in date_candidates if c in sample), None)
    
    if not date_col:
        print("Warning: Date column not found. Defaulting to full fetch...")
        final_url = f"{base_url}?$limit=100000"
    else:
        # Incremental window (last 2 years)
        lookback = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%dT%H:%M:%S')
        final_url = f"{base_url}?$where={date_col} > '{lookback}'&$limit=100000"
        print(f"System: Filtering by '{date_col}' > {lookback}")

    response = requests.get(final_url, headers=headers, timeout=30)
    response.raise_for_status()
    
    df = pd.DataFrame(response.json())
    if len(df) < 500:
        raise ValueError("DQ FAILURE: Insufficient records returned.")
    return df

def categorize_sector(license_text):
    text = str(license_text).upper()
    for sector, keywords in SECTOR_MAPPING.items():
        if any(kw in text for kw in keywords):
            return sector
    return 'GENERAL_COMMERCIAL'

def transform_and_aggregate(df):
    df.columns = [c.lower() for c in df.columns]
    
    # Identify key columns dynamically
    comm_col = next((c for c in ['communityname', 'community', 'comm_name'] if c in df.columns), 'community_name')
    type_col = next((c for c in ['licencetypes', 'licence_type'] if c in df.columns), 'licence_type')
    date_col = next((c for c in ['issueddate', 'issued_date', 'date_issued'] if c in df.columns), None)

    df['sector'] = df[type_col].apply(categorize_sector)
    
    # KPI Logic
    if date_col:
        df['issued_dt'] = pd.to_datetime(df[date_col], errors='coerce')
        momentum_limit = datetime.now() - timedelta(days=365)
        momentum_calc = lambda x: (x > momentum_limit).sum()
    else:
        df['issued_dt'] = datetime.now()
        momentum_calc = lambda x: 0

    agg = df.groupby(comm_col).agg(
        footprint=('sector', 'count'),
        resilience=('sector', 'nunique'),
        momentum=('issued_dt', momentum_calc)
    ).reset_index()

    # Normalization & Scores
    for col in ['footprint', 'resilience', 'momentum']:
        max_val = agg[col].max()
        agg[f'n_{col}'] = agg[col] / (max_val if max_val > 0 else 1)

    agg['n_acceleration'] = (agg['n_momentum'] * agg['n_resilience'])
    agg['n_acceleration'] /= (agg['n_acceleration'].max() if agg['n_acceleration'].max() > 0 else 1)

    agg['health_score'] = (
        (agg['n_footprint'] * 0.35) + 
        (agg['n_resilience'] * 0.35) + 
        (agg['n_momentum'] * 0.15) + 
        (agg['n_acceleration'] * 0.15)
    )

    agg['strategic_action'] = pd.cut(
        agg['health_score'], 
        bins=[0, 0.25, 0.50, 0.75, 1.05], 
        labels=["URGENT INTERVENTION", "MONITOR FRICTION", "STABLE OPERATIONS", "NEURAL CORE"],
        include_lowest=True
    )
    
    return agg.rename(columns={comm_col: 'community_name'})

if __name__ == "__main__":
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)
    
    try:
        raw_data = fetch_calgary_data()
        processed_data = transform_and_aggregate(raw_data)
        processed_data.to_csv(os.path.join(data_dir, "nexus_intelligence_feed.csv"), index=False)
        print(f"Success: Processed {len(processed_data)} community nodes.")
    except Exception as e:
        print(f"Pipeline Critical Failure: {e}")
        exit(1)
