import pandas as pd
import os
import requests
from datetime import datetime, timedelta

# --- CONFIGURATION & DQ GATES ---
SECTOR_MAPPING = {
    'Energy & Resources': ['OIL', 'GAS', 'ENERGY', 'MINING', 'PETROLEUM'],
    'Hospitality & Tourism': ['RESTAURANT', 'FOOD', 'HOTEL', 'PUB', 'CATER', 'ALCOHOL'],
    'Construction & Infra': ['CONSTRUCT', 'BUILD', 'CONTRACTOR', 'PLUMB', 'ELECTRIC'],
    'Retail Trade': ['RETAIL', 'DEALER', 'STORE', 'SHOP', 'SALES'],
    'Finance & Insurance': ['FINANCE', 'BANK', 'INSURANCE', 'INVEST', 'MORTGAGE'],
    'Professional Services': ['CONSULT', 'LEGAL', 'ACCOUNT', 'ENGINEER', 'ARCHITECT'],
    'Tech & Innovation': ['SOFTWARE', 'TECH', 'DATA', 'SYSTEM', 'COMPUTER'],
    'Healthcare & Wellness': ['HEALTH', 'MEDICAL', 'DENTAL', 'CLINIC', 'HOSPITAL'],
    'Personal Services': ['MASSAGE', 'SALON', 'BARBER', 'CLEAN', 'LAUNDRY'],
    'Logistics & Transport': ['WAREHOUSE', 'TRUCK', 'TRANSPORT', 'LOGISTIC'],
    'Industrial & Mfg': ['MANUFACTUR', 'FACTORY', 'INDUSTRIAL', 'MACHINE'],
    'Real Estate': ['REAL ESTATE', 'PROPERTY', 'LEASING', 'RENTAL'],
    'Education': ['SCHOOL', 'TRAIN', 'EDUCATE', 'ACADEMY', 'TUTOR'],
    'Public & Social': ['GOVERNMENT', 'PUBLIC', 'SOCIAL', 'COMMUNITY'],
    'Arts & Recreation': ['ART', 'MUSEUM', 'RECREATION', 'GYM', 'FITNESS'],
    'Automotive': ['AUTO', 'VEHICLE', 'REPAIR', 'CAR WASH', 'TIRE'],
    'Agri & Environment': ['AGRI', 'FARM', 'GARDEN', 'ENVIRONMENT', 'WASTE'],
    'Media & Comm': ['MEDIA', 'PUBLISH', 'COMMUNICATION', 'TELECOM'],
    'Security & Safety': ['SECURITY', 'SAFETY', 'ALARM', 'INVESTIGATE'],
    'Cannabis & Tobacco': ['CANNABIS', 'TOBACCO', 'VAPE']
}

MIN_EXPECTED_RECORDS = 500  # DQ Gate: Minimal data volume before failure

def fetch_incremental_data():
    """Fetches data using a rolling window to reduce API load and improve scalability."""
    # Using the system-link for Business Licenses (alias is more stable than 4x4 ID)
    # We filter on the server side using SoQL $where to only pull the last 3 years of data
    three_years_ago = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%dT%H:%M:%S')
    
    url = f"https://data.calgary.ca/resource/fmxu-7969.json?$where=issueddate > '{three_years_ago}'&$limit=100000"
    
    print(f"System: Fetching incremental data since {three_years_ago}...")
    response = requests.get(url, headers={'User-Agent': 'NexusStrategicIntelligence/2.0'})
    response.raise_for_status()
    
    df = pd.DataFrame(response.json())
    
    # --- DATA QUALITY GATE 1: VOLUME ---
    if len(df) < MIN_EXPECTED_RECORDS:
        raise ValueError(f"DQ Failure: API returned only {len(df)} records. Aborting to protect dashboard.")
        
    return df

def categorize_sector(license_text):
    """Aggregates string values based on the global SECTOR_MAPPING dictionary."""
    text = str(license_text).upper()
    for sector, keywords in SECTOR_MAPPING.items():
        if any(kw in text for kw in keywords):
            return sector
    return 'GENERAL_COMMERCIAL'

def calculate_intelligence_metrics(df):
    """Performs vector-based KPI calculations with weighted accuracy logic."""
    df.columns = [c.lower() for c in df.columns]
    
    # Identify dynamic columns
    comm_col = next((c for c in ['communityname', 'community', 'comm_name'] if c in df.columns), 'community_name')
    type_col = next((c for c in ['licencetypes', 'licence_type'] if c in df.columns), 'licence_type')
    date_col = next((c for c in ['issueddate', 'issued_date'] if c in df.columns), 'issued_date')

    # Data Cleanup
    df = df.dropna(subset=[type_col])
    df['sector'] = df[type_col].apply(categorize_sector)
    df['issued_dt'] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Momentum Threshold: Activity in the last 12 months
    one_year_ago = datetime.now() - timedelta(days=365)
    
    # Aggregation
    agg = df.groupby(comm_col).agg(
        footprint=('sector', 'count'),
        resilience=('sector', 'nunique'),
        momentum=('issued_dt', lambda x: (x > one_year_ago).sum())
    ).reset_index()

    # Normalization (0 to 1 scaling)
    for col in ['footprint', 'resilience', 'momentum']:
        max_val = agg[col].max()
        agg[f'n_{col}'] = agg[col] / (max_val if max_val > 0 else 1)

    # Innovation Acceleration (Momentum x Resilience)
    agg['n_acceleration'] = (agg['n_momentum'] * agg['n_resilience'])
    acc_max = agg['n_acceleration'].max()
    agg['n_acceleration'] /= (acc_max if acc_max > 0 else 1)

    # Weighted Health Score: Prioritizing High-Accuracy historical metrics (Footprint/Resilience)
    agg['health_score'] = (
        (agg['n_footprint'] * 0.35) + 
        (agg['n_resilience'] * 0.35) + 
        (agg['n_momentum'] * 0.15) + 
        (agg['n_acceleration'] * 0.15)
    )

    # --- DATA QUALITY GATE 2: HEALTH SCORE INTEGRITY ---
    if agg['health_score'].isnull().all():
        raise ValueError("DQ Failure: Intelligence Engine produced null Health Scores.")

    # Strategic Directives
    agg['strategic_action'] = pd.cut(
        agg['health_score'], 
        bins=[0, 0.25, 0.50, 0.75, 1.05], 
        labels=["URGENT INTERVENTION", "MONITOR FRICTION", "STABLE OPERATIONS", "NEURAL CORE"],
        include_lowest=True
    )
    
    return agg.rename(columns={comm_col: 'community_name'})

if __name__ == "__main__":
    base_path = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(base_path, "..", "data", "nexus_intelligence_feed.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    try:
        raw_df = fetch_incremental_data()
        processed_df = calculate_intelligence_metrics(raw_df)
        processed_df.to_csv(output_path, index=False)
        print(f"Pipeline Success: Nexus Feed contains {len(processed_df)} community nodes.")
    except Exception as e:
        print(f"Critical System Failure: {e}")
        exit(1)
