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

# --- DATA QUALITY GATES ---
MIN_RECORDS_THRESHOLD = 1000  # Fail if dataset seems suspiciously small

def fetch_calgary_data():
    """Fetches data from the 2026 production endpoint with server-side filtering."""
    # New verified 2026 Resource ID
    resource_id = "vdjc-pybd" 
    
    # Performance: Incremental window (last 2 years) to reduce payload size
    lookback_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%dT%H:%M:%S')
    
    # Server-side SoQL filtering: Only fetch what we need
    url = f"https://data.calgary.ca/resource/{resource_id}.json?$where=issueddate > '{lookback_date}'&$limit=100000"
    
    print(f"System: Accessing Production Node {resource_id}...")
    print(f"System: Filtering records issued after {lookback_date}...")
    
    response = requests.get(url, headers={'User-Agent': 'NexusStrategicIntelligence/2.1'}, timeout=30)
    response.raise_for_status()
    
    df = pd.DataFrame(response.json())
    
    if len(df) < MIN_RECORDS_THRESHOLD:
        raise ValueError(f"DQ FAILURE: API returned only {len(df)} records. Safety shutdown to protect dashboard integrity.")
        
    return df

def categorize_sector(license_text):
    text = str(license_text).upper()
    for sector, keywords in SECTOR_MAPPING.items():
        if any(kw in text for kw in keywords):
            return sector
    return 'GENERAL_COMMERCIAL'

def transform_and_aggregate(df):
    """Performs KPI calculations and health score logic."""
    df.columns = [c.lower() for c in df.columns]
    
    # Fuzzy column matching for resilience
    comm_col = next((c for c in ['communityname', 'community', 'comm_name'] if c in df.columns), 'community_name')
    type_col = next((c for c in ['licencetypes', 'licence_type'] if c in df.columns), 'licence_type')
    date_col = next((c for c in ['issueddate', 'issued_date'] if c in df.columns), 'issued_date')

    df['sector'] = df[type_col].apply(categorize_sector)
    df['issued_dt'] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Momentum: Last 12 months
    momentum_limit = datetime.now() - timedelta(days=365)
    
    # Aggregation Engine
    agg = df.groupby(comm_col).agg(
        footprint=('sector', 'count'),
        resilience=('sector', 'nunique'),
        momentum=('issued_dt', lambda x: (x > momentum_limit).sum())
    ).reset_index()

    # Scaling Logic
    for col in ['footprint', 'resilience', 'momentum']:
        max_val = agg[col].max()
        agg[f'n_{col}'] = agg[col] / (max_val if max_val > 0 else 1)

    # Innovation Acceleration KPI
    agg['n_acceleration'] = (agg['n_momentum'] * agg['n_resilience'])
    agg['n_acceleration'] /= (agg['n_acceleration'].max() if agg['n_acceleration'].max() > 0 else 1)

    # Strategic Health Score (Weighted 35/35/15/15)
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
        
        output_file = os.path.join(data_dir, "nexus_intelligence_feed.csv")
        processed_data.to_csv(output_file, index=False)
        print(f"Success: Processed {len(processed_data)} community nodes.")
    except Exception as e:
        print(f"Pipeline Critical Failure: {e}")
        exit(1)
