import pandas as pd
import os
import requests

def fetch_strategic_data():
    """Fetches business license data using the most stable known endpoint."""
    # Current active endpoint for Calgary Business Licenses
    url = "https://data.calgary.ca/resource/6963-8pqa.json?$limit=100000"
    
    print("System: Accessing Calgary Strategic Data Portal...")
    response = requests.get(url)
    
    # If 404, try the secondary stable endpoint
    if response.status_code == 404:
        print("System: Primary endpoint moved. Redirecting to secondary...")
        url = "https://data.calgary.ca/resource/g5zp-yj93.json?$limit=100000"
        response = requests.get(url)
        
    response.raise_for_status()
    return pd.DataFrame(response.json())

def categorize_sector(license_text):
    """STRICT LOGIC: Aggregates 'licencetypes' into ~20 strategic sectors."""
    text = str(license_text).upper()
    mappings = {
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
        'Automotive': ['AUTO', 'VEHICLE', 'REPAIR', 'CAR WASH'],
        'Agri & Environment': ['AGRI', 'FARM', 'GARDEN', 'ENVIRONMENT', 'WASTE'],
        'Media & Comm': ['MEDIA', 'PUBLISH', 'COMMUNICATION', 'TELECOM'],
        'Security & Safety': ['SECURITY', 'SAFETY', 'ALARM', 'INVESTIGATE'],
        'Cannabis & Tobacco': ['CANNABIS', 'TOBACCO', 'VAPE']
    }
    for sector, keywords in mappings.items():
        if any(kw in text for kw in keywords):
            return sector
    return 'GENERAL_COMMERCIAL'

def process_nexus_feed(df):
    """Generates weighted KPIs and Health Scores."""
    # Standardize column names
    df.columns = [c.lower() for c in df.columns]
    
    # Ensure necessary columns exist
    required = ['communityname', 'licencetypes', 'issueddate']
    df = df.dropna(subset=[col for col in required if col in df.columns])
    
    df['sector'] = df['licencetypes'].apply(categorize_sector)
    df['issueddate'] = pd.to_datetime(df['issueddate'], errors='coerce')
    
    # Community Aggregation
    agg = df.groupby('communityname').agg(
        footprint=('licencetypes', 'count'),
        resilience=('sector', 'nunique'),
        momentum=('issueddate', lambda x: (x > '2025-01-01').sum())
    ).reset_index()

    # Normalization
    for col in ['footprint', 'resilience', 'momentum']:
        max_val = agg[col].max()
        agg[f'n_{col}'] = agg[col] / (max_val if max_val > 0 else 1)

    # Innovation Acceleration
    agg['n_acceleration'] = (agg['n_momentum'] * agg['n_resilience'])
    acc_max = agg['n_acceleration'].max()
    agg['n_acceleration'] /= (acc_max if acc_max > 0 else 1)

    # Weighted Health Score (35/35/15/15)
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
    
    return agg

if __name__ == "__main__":
    base_path = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_path, "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        raw_df = fetch_strategic_data()
        final_df = process_nexus_feed(raw_df)
        final_df.to_csv(os.path.join(output_dir, "nexus_intelligence_feed.csv"), index=False)
        print("Success: Nexus Intelligence Feed updated.")
    except Exception as e:
        print(f"Pipeline Error: {e}")
        exit(1)
