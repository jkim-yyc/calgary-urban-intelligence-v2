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
    """Fetches data using verified 2026 production column names."""
    resource_id = "vdjc-pybd" 
    base_url = f"https://data.calgary.ca/resource/{resource_id}.json"
    headers = {'User-Agent': 'NexusStrategicIntelligence/2.3'}
    
    # Verified 2026 Schema Fields
    # comdistnm = Community District Name
    # first_iss_dt = First Issued Date
    # licencetypes = Licence Type
    
    # Filter for the last 2 years to ensure performance
    lookback = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%dT%H:%M:%S')
    
    # We use 'first_iss_dt' for the SoQL filter
    final_url = f"{base_url}?$where=first_iss_dt > '{lookback}'&$limit=100000"
    
    print(f"System: Accessing Production Node {resource_id}...")
    print(f"System: Filtering by 'first_iss_dt' > {lookback}")

    response = requests.get(final_url, headers=headers, timeout=30)
    response.raise_for_status()
    
    df = pd.DataFrame(response.json())
    
    if df.empty:
        raise ValueError("DQ FAILURE: API returned an empty dataset for the selected period.")
    
    return df

def categorize_sector(license_text):
    text = str(license_text).upper()
    for sector, keywords in SECTOR_MAPPING.items():
        if any(kw in text for kw in keywords):
            return sector
    return 'GENERAL_COMMERCIAL'

def transform_and_aggregate(df):
    """Aggregates data using verified Calgary API field names."""
    # Convert all columns to lowercase for consistent internal handling
    df.columns = [c.lower() for c in df.columns]
    
    # 2026 Field Mapping
    comm_col = 'comdistnm'    # Community Name
    type_col = 'licencetypes' # License Types
    date_col = 'first_iss_dt'  # Issue Date
    
    if comm_col not in df.columns or type_col not in df.columns:
        available = df.columns.tolist()
        raise KeyError(f"Schema mismatch. Expected '{comm_col}', found: {available}")

    df['sector'] = df[type_col].apply(categorize_sector)
    df['issued_dt'] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Momentum: Activity in the last 12 months
    momentum_limit = datetime.now() - timedelta(days=365)
    
    # Aggregation logic
    agg = df.groupby(comm_col).agg(
        footprint=('sector', 'count'),
        resilience=('sector', 'nunique'),
        momentum=('issued_dt', lambda x: (x > momentum_limit).sum())
    ).reset_index()

    # Normalization (0 to 1)
    for col in ['footprint', 'resilience', 'momentum']:
        max_val = agg[col].max()
        agg[f'n_{col}'] = agg[col] / (max_val if max_val > 0 else 1)

    # Innovation Acceleration KPI
    agg['n_acceleration'] = (agg['n_momentum'] * agg['n_resilience'])
    acc_max = agg['n_acceleration'].max()
    agg['n_acceleration'] /= (acc_max if acc_max > 0 else 1)

    # Weighted Strategic Health Score
    agg['health_score'] = (
        (agg['n_footprint'] * 0.35) + 
        (agg['n_resilience'] * 0.35) + 
        (agg['n_momentum'] * 0.15) + 
        (agg['n_acceleration'] * 0.15)
    )

    # Directives
    agg['strategic_action'] = pd.cut(
        agg['health_score'], 
        bins=[0, 0.25, 0.50, 0.75, 1.05], 
        labels=["URGENT INTERVENTION", "MONITOR FRICTION", "STABLE OPERATIONS", "NEURAL CORE"],
        include_lowest=True
    )
    
    # Return with a clean user-friendly community column name
    return agg.rename(columns={comm_col: 'community_name'})

if __name__ == "__main__":
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_path, "..", "data")
    os.makedirs(data_dir, exist_ok=True)
    
    try:
        raw_data = fetch_calgary_data()
        processed_data = transform_and_aggregate(raw_data)
        
        output_path = os.path.join(data_dir, "nexus_intelligence_feed.csv")
        processed_data.to_csv(output_path, index=False)
        print(f"Success: Processed {len(processed_data)} community nodes.")
    except Exception as e:
        print(f"Pipeline Critical Failure: {e}")
        exit(1)
