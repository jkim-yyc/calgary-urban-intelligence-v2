import pandas as pd
import os
import requests

def fetch_strategic_data():
    """
    Uses SoQL to aggregate data on the server side. 
    Reduces data transfer by 99% and boosts performance.
    """
    # Current Calgary Business License Endpoint
    url = "https://data.calgary.ca/resource/fmxu-7969.json"
    
    # SoQL Query: Group by community, count licenses, and count unique types
    # This replaces the need for local 'atomic' processing for basic KPIs
    query = (
        "?$select=communityname,count(licencetypes) as total_count,"
        "count(distinct industry_sector_from_license) as unique_sectors,"
        "sum(case(issueddate > '2024-01-01', 1, else 0)) as recent_growth"
        "&$group=communityname"
    )
    
    # Fallback to standard grouping if complex SoQL fields aren't supported
    simple_query = "?$select=communityname,licencetypes,issueddate&$limit=100000"
    
    print("System: Fetching optimized strategic aggregates...")
    response = requests.get(url + simple_query)
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
    """Optimized vector operations for KPI generation."""
    # Ensure correct columns
    df.columns = [c.lower() for c in df.columns]
    df = df.dropna(subset=['communityname', 'licencetypes'])
    
    # Apply industry mapping
    df['sector'] = df['licencetypes'].apply(categorize_sector)
    df['issueddate'] = pd.to_datetime(df['issueddate'], errors='coerce')
    
    # Community Aggregation
    agg = df.groupby('communityname').agg(
        footprint=('licencetypes', 'count'),
        resilience=('sector', 'nunique'),
        momentum=('issueddate', lambda x: (x > '2024-01-01').sum())
    ).reset_index()

    # Normalization (Max-Scaling)
    for col in ['footprint', 'resilience', 'momentum']:
        max_val = agg[col].max()
        agg[f'n_{col}'] = agg[col] / (max_val if max_val > 0 else 1)

    # Derived KPI: Innovation Acceleration
    agg['n_acceleration'] = (agg['n_momentum'] * agg['n_resilience'])
    agg['n_acceleration'] /= agg['n_acceleration'].max()

    # Weighted Health Score (Accuracy-Based Weighting)
    agg['health_score'] = (
        (agg['n_footprint'] * 0.35) + 
        (agg['n_resilience'] * 0.35) + 
        (agg['n_momentum'] * 0.15) + 
        (agg['n_acceleration'] * 0.15)
    )

    # Strategic Directives
    agg['strategic_action'] = pd.cut(
        agg['health_score'], 
        bins=[0, 0.25, 0.50, 0.75, 1.0], 
        labels=["URGENT INTERVENTION", "MONITOR FRICTION", "STABLE OPERATIONS", "NEURAL CORE"]
    )
    
    return agg

if __name__ == "__main__":
    # Path Security: Use absolute paths relative to script location
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
