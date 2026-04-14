import pandas as pd
import os
import requests
from datetime import datetime

def fetch_strategic_data():
    """Fetches Calgary Business License data using the current stable 2026 endpoint."""
    # Current master dataset for Calgary Business Licenses
    url = "https://data.calgary.ca/resource/tp68-p4gg.json?$limit=100000"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) NexusStrategicIntelligence/1.0'
    }

    print("System: Accessing Calgary Strategic Data Portal...")
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        # Fallback to the 'Open Data' discovery endpoint if the resource ID has rotated
        if response.status_code == 404:
            print("System: Primary endpoint moved. Searching Discovery API...")
            url = "https://data.calgary.ca/resource/6963-8pqa.json?$limit=100000"
            response = requests.get(url, headers=headers, timeout=30)
            
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"System: Strategic Data Access Denied. Reason: {e}")
        raise

def categorize_sector(license_text):
    """STRICT LOGIC: Aggregates 'licencetypes' into ~20 strategic sectors."""
    text = str(license_text).upper()
    mappings = {
        'Energy & Resources': ['OIL', 'GAS', 'ENERGY', 'MINING', 'PETROLEUM', 'SOLAR', 'WIND'],
        'Hospitality & Tourism': ['RESTAURANT', 'FOOD', 'HOTEL', 'PUB', 'CATER', 'ALCOHOL', 'BREWERY'],
        'Construction & Infra': ['CONSTRUCT', 'BUILD', 'CONTRACTOR', 'PLUMB', 'ELECTRIC', 'ROOFING'],
        'Retail Trade': ['RETAIL', 'DEALER', 'STORE', 'SHOP', 'SALES'],
        'Finance & Insurance': ['FINANCE', 'BANK', 'INSURANCE', 'INVEST', 'MORTGAGE', 'BROKER'],
        'Professional Services': ['CONSULT', 'LEGAL', 'ACCOUNT', 'ENGINEER', 'ARCHITECT'],
        'Tech & Innovation': ['SOFTWARE', 'TECH', 'DATA', 'SYSTEM', 'COMPUTER', 'CYBER'],
        'Healthcare & Wellness': ['HEALTH', 'MEDICAL', 'DENTAL', 'CLINIC', 'HOSPITAL', 'PHARMACY'],
        'Personal Services': ['MASSAGE', 'SALON', 'BARBER', 'CLEAN', 'LAUNDRY', 'AESTHETIC'],
        'Logistics & Transport': ['WAREHOUSE', 'TRUCK', 'TRANSPORT', 'LOGISTIC', 'FREIGHT'],
        'Industrial & Mfg': ['MANUFACTUR', 'FACTORY', 'INDUSTRIAL', 'MACHINE', 'FABRICATION'],
        'Real Estate': ['REAL ESTATE', 'PROPERTY', 'LEASING', 'RENTAL', 'LANDLORD'],
        'Education': ['SCHOOL', 'TRAIN', 'EDUCATE', 'ACADEMY', 'TUTOR', 'COLLEGE'],
        'Public & Social': ['GOVERNMENT', 'PUBLIC', 'SOCIAL', 'COMMUNITY', 'NON-PROFIT'],
        'Arts & Recreation': ['ART', 'MUSEUM', 'RECREATION', 'GYM', 'FITNESS', 'SPORTS'],
        'Automotive': ['AUTO', 'VEHICLE', 'REPAIR', 'CAR WASH', 'TIRE', 'MECHANIC'],
        'Agri & Environment': ['AGRI', 'FARM', 'GARDEN', 'ENVIRONMENT', 'WASTE', 'RECYCLE'],
        'Media & Comm': ['MEDIA', 'PUBLISH', 'COMMUNICATION', 'TELECOM', 'ADVERT'],
        'Security & Safety': ['SECURITY', 'SAFETY', 'ALARM', 'INVESTIGATE'],
        'Cannabis & Tobacco': ['CANNABIS', 'TOBACCO', 'VAPE']
    }
    for sector, keywords in mappings.items():
        if any(kw in text for kw in keywords):
            return sector
    return 'GENERAL_COMMERCIAL'

def process_nexus_feed(df):
    """Generates weighted KPIs and Health Scores for Community-level mapping."""
    df.columns = [c.lower() for c in df.columns]
    
    # Check for alternate naming conventions in the Calgary API
    comm_col = 'communityname' if 'communityname' in df.columns else 'community'
    type_col = 'licencetypes' if 'licencetypes' in df.columns else 'licence_type'
    date_col = 'issueddate' if 'issueddate' in df.columns else 'issued_date'

    df = df.dropna(subset=[type_col])
    df['community_key'] = df[comm_col].fillna('CITYWIDE')
    df['sector'] = df[type_col].apply(categorize_sector)
    df['issued_dt'] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Momentum: Activity since Jan 1, 2025
    momentum_cutoff = datetime(2025, 1, 1)
    
    # Aggregation
    agg = df.groupby('community_key').agg(
        footprint=('sector', 'count'),
        resilience=('sector', 'nunique'),
        momentum=('issued_dt', lambda x: (x > momentum_cutoff).sum())
    ).reset_index()

    # Normalization
    for col in ['footprint', 'resilience', 'momentum']:
        max_val = agg[col].max()
        agg[f'n_{col}'] = agg[col] / (max_val if max_val > 0 else 1)

    # Innovation Acceleration
    agg['n_acceleration'] = (agg['n_momentum'] * agg['n_resilience'])
    acc_max = agg['n_acceleration'].max()
    agg['n_acceleration'] /= (acc_max if acc_max > 0 else 1)

    # Weighted Health Score: 35/35/15/15
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
    
    return agg.rename(columns={'community_key': 'community_name'})

if __name__ == "__main__":
    base_path = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_path, "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        raw_df = fetch_strategic_data()
        final_df = process_nexus_feed(raw_df)
        final_df.to_csv(os.path.join(output_dir, "nexus_intelligence_feed.csv"), index=False)
        print("Success: Nexus Intelligence Feed generated.")
    except Exception as e:
        print(f"Pipeline Error: {e}")
        exit(1)
