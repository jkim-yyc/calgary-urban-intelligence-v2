import pandas as pd
import numpy as np
import os
import requests
from datetime import datetime, timedelta

def fetch_all_calgary_data():
    """Extracts the full atomic dataset from Calgary Open Data Portal."""
    url = "https://data.calgary.ca/resource/6963-8pqa.json?$limit=500000"
    try:
        response = requests.get(url)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        return df
    except Exception as e:
        print(f"Extraction Failed: {e}")
        exit(1)

def categorize_sector(license_text):
    """
    STRICT LOGIC: Scans 'licencetypes' and maps to ~20 strategic industry sectors.
    Designed for high-efficiency string aggregation.
    """
    text = str(license_text).upper()
    mappings = {
        'Energy & Resources': ['OIL', 'GAS', 'ENERGY', 'MINING', 'PETROLEUM', 'EXTRACT', 'RENEWABLE'],
        'Hospitality & Tourism': ['RESTAURANT', 'FOOD', 'HOTEL', 'PUB', 'CATER', 'ALCOHOL', 'BEVERAGE', 'BREWERY'],
        'Construction & Infra': ['CONSTRUCT', 'BUILD', 'CONTRACTOR', 'PLUMB', 'ELECTRIC', 'ROOFING', 'EXCAVATE'],
        'Retail Trade': ['RETAIL', 'DEALER', 'STORE', 'SHOP', 'SALES', 'MERCHANDISE', 'MALL'],
        'Finance & Insurance': ['FINANCE', 'BANK', 'INSURANCE', 'INVEST', 'MORTGAGE', 'BROKER'],
        'Professional Services': ['CONSULT', 'LEGAL', 'ACCOUNT', 'ENGINEER', 'ARCHITECT', 'ADVISOR'],
        'Tech & Innovation': ['SOFTWARE', 'TECH', 'DATA', 'SYSTEM', 'COMPUTER', 'CYBER', 'DIGITAL'],
        'Healthcare & Wellness': ['HEALTH', 'MEDICAL', 'DENTAL', 'CLINIC', 'HOSPITAL', 'PHARMACY', 'OPTICAL'],
        'Personal Services': ['MASSAGE', 'SALON', 'BARBER', 'CLEAN', 'LAUNDRY', 'AESTHETIC', 'TATTOO'],
        'Logistics & Transport': ['WAREHOUSE', 'TRUCK', 'TRANSPORT', 'LOGISTIC', 'FREIGHT', 'COURIER'],
        'Industrial & Mfg': ['MANUFACTUR', 'FACTORY', 'INDUSTRIAL', 'MACHINE', 'FABRICATION', 'WELDING'],
        'Real Estate': ['REAL ESTATE', 'PROPERTY', 'LEASING', 'RENTAL', 'LANDLORD', 'REALTOR'],
        'Education': ['SCHOOL', 'TRAIN', 'EDUCATE', 'ACADEMY', 'TUTOR', 'COLLEGE', 'UNIVERSITY'],
        'Public & Social': ['GOVERNMENT', 'PUBLIC', 'SOCIAL', 'COMMUNITY', 'NON-PROFIT', 'CHARITY'],
        'Arts & Recreation': ['ART', 'MUSEUM', 'RECREATION', 'GYM', 'FITNESS', 'SPORTS', 'THEATRE'],
        'Automotive': ['AUTO', 'VEHICLE', 'REPAIR', 'CAR WASH', 'TIRE', 'MECHANIC'],
        'Agri & Environment': ['AGRI', 'FARM', 'GARDEN', 'ENVIRONMENT', 'WASTE', 'RECYCLE', 'LANDSCAPE'],
        'Media & Comm': ['MEDIA', 'PUBLISH', 'COMMUNICATION', 'TELECOM', 'ADVERT', 'BROADCAST'],
        'Security & Safety': ['SECURITY', 'SAFETY', 'ALARM', 'INVESTIGATE', 'PROTECT'],
        'Cannabis & Tobacco': ['CANNABIS', 'TOBACCO', 'VAPE', 'RETAIL CANNABIS']
    }
    for sector, keywords in mappings.items():
        if any(kw in text for kw in keywords):
            return sector
    return 'GENERAL_COMMERCIAL'

def process_community_intelligence(df):
    """Processes atomic data into a Weighted 4-KPI Community Model."""
    
    # Clean and Format
    df = df.dropna(subset=['communityname', 'licencetypes', 'issueddate'])
    df['industry_sector'] = df['licencetypes'].apply(categorize_sector)
    df['issueddate'] = pd.to_datetime(df['issueddate'])
    
    # Define Recent Growth (Last 2 Years)
    two_years_ago = datetime.now() - timedelta(days=730)
    df['is_recent'] = df['issueddate'] > two_years_ago

    # 1. Atomic to Community Aggregation
    community_agg = df.groupby('communityname').agg(
        total_count=('licencetypes', 'count'),
        unique_sectors=('industry_sector', 'nunique'),
        recent_growth=('is_recent', 'sum')
    ).reset_index()

    # 2. KPI Normalization
    community_agg['strategic_footprint'] = community_agg['total_count'] / community_agg['total_count'].max()
    community_agg['systemic_resilience'] = community_agg['unique_sectors'] / community_agg['unique_sectors'].max()
    community_agg['expansion_momentum'] = community_agg['recent_growth'] / community_agg['recent_growth'].max()
    
    # Innovation Acceleration logic
    community_agg['innovation_acceleration'] = (community_agg['expansion_momentum'] * community_agg['systemic_resilience'])
    community_agg['innovation_acceleration'] = community_agg['innovation_acceleration'] / community_agg['innovation_acceleration'].max()

    # 3. Weighted Health Score Calculation
    # Weights: Footprint (35%) & Resilience (35%) are high accuracy.
    # Momentum (15%) & Acceleration (15%) account for temporal volatility.
    community_agg['health_score'] = (
        (community_agg['strategic_footprint'] * 0.35) + 
        (community_agg['systemic_resilience'] * 0.35) + 
        (community_agg['expansion_momentum'] * 0.15) + 
        (community_agg['innovation_acceleration'] * 0.15)
    )

    # 4. Strategic Action Labels
    def get_directive(score):
        if score >= 0.75: return "NEURAL CORE"
        elif 0.50 <= score < 0.75: return "STABLE OPERATIONS"
        elif 0.25 <= score < 0.50: return "MONITOR FRICTION"
        else: return "URGENT INTERVENTION"

    community_agg['strategic_action'] = community_agg['health_score'].apply(get_directive)
    
    return community_agg

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.abspath(os.path.join(script_dir, '..'))
    data_dir = os.path.join(root_dir, 'data')
    
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    output_path = os.path.join(data_dir, 'nexus_intelligence_feed.csv')

    raw_data = fetch_all_calgary_data()
    processed_feed = process_community_intelligence(raw_data)
    processed_feed.to_csv(output_path, index=False)
    print(f"Success: Production Feed generated at {output_path}")
