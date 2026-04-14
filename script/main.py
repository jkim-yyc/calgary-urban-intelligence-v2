import pandas as pd
import numpy as np
import os

def categorize_sector(license_text):
    text = str(license_text).upper()
    mappings = {
        'Energy & Resources': ['OIL', 'GAS', 'ENERGY', 'MINING', 'EXTRACT', 'PETROLEUM'],
        'Hospitality & Tourism': ['RESTAURANT', 'FOOD', 'ALCOHOL', 'BEVERAGE', 'HOTEL', 'CATER', 'PUB'],
        'Construction & Infra': ['CONSTRUCT', 'BUILD', 'CONTRACTOR', 'DEVELOP', 'PLUMB', 'ELECTRIC'],
        'Retail Trade': ['RETAIL', 'DEALER', 'STORE', 'SHOP', 'SALES', 'MERCHANDISE'],
        'Finance & Insurance': ['FINANCE', 'BANK', 'INSURANCE', 'INVEST', 'MORTGAGE'],
        'Professional Services': ['CONSULT', 'LEGAL', 'ACCOUNT', 'ENGINEER', 'ARCHITECT'],
        'Tech & Innovation': ['SOFTWARE', 'TECH', 'DATA', 'SYSTEM', 'COMPUTER', 'CYBER'],
        'Healthcare & Wellness': ['HEALTH', 'MEDICAL', 'DENTAL', 'CLINIC', 'HOSPITAL', 'WELLNESS'],
        'Personal Services': ['MASSAGE', 'SALON', 'BARBER', 'CLEAN', 'LAUNDRY', 'AESTHETIC'],
        'Logistics & Transport': ['WAREHOUSE', 'DISTRIBUTION', 'TRUCK', 'TRANSPORT', 'FREIGHT', 'LOGISTIC'],
        'Industrial & Mfg': ['MANUFACTUR', 'FACTORY', 'INDUSTRIAL', 'MACHINE', 'FABRICATION'],
        'Real Estate': ['REAL ESTATE', 'PROPERTY', 'LEASING', 'RENTAL', 'LANDLORD'],
        'Education': ['SCHOOL', 'TRAIN', 'EDUCATE', 'ACADEMY', 'TUTOR'],
        'Public & Social': ['GOVERNMENT', 'PUBLIC', 'SOCIAL', 'COMMUNITY', 'NON-PROFIT'],
        'Arts & Recreation': ['ART', 'MUSEUM', 'RECREATION', 'GYM', 'FITNESS', 'SPORTS'],
        'Automotive': ['AUTO', 'VEHICLE', 'REPAIR', 'CAR WASH', 'TIRE'],
        'Agri & Environment': ['AGRI', 'FARM', 'GARDEN', 'ENVIRONMENT', 'WASTE', 'RECYCLE'],
        'Media & Comm': ['MEDIA', 'PUBLISH', 'COMMUNICATION', 'TELECOM', 'ADVERT'],
        'Security & Safety': ['SECURITY', 'SAFETY', 'ALARM', 'INVESTIGATE'],
        'Cannabis & Tobacco': ['CANNABIS', 'TOBACCO', 'VAPE', 'RETAIL CANNABIS']
    }
    for sector, keywords in mappings.items():
        if any(kw in text for kw in keywords):
            return sector
    return 'GENERAL_COMMERCIAL'

def calculate_nexus_metrics(df):
    # Ensure licencetypes exists, handle case sensitivity
    col_name = 'licencetypes' if 'licencetypes' in df.columns else 'LICENCETYPES'
    df['industry_sector'] = df[col_name].apply(categorize_sector)

    df['strategic_footprint'] = df['impact_weight'] / df['impact_weight'].max()
    df['systemic_resilience'] = df['vitality_index'] / df['vitality_index'].max()
    df['expansion_momentum'] = df['growth_count'] / df['growth_count'].max()

    city_avg_growth = df['growth_count'].mean()
    df['innovation_acceleration'] = (df['growth_count'] / (city_avg_growth if city_avg_growth != 0 else 1)) * df['systemic_resilience']
    df['innovation_acceleration'] = df['innovation_acceleration'].clip(upper=2.0)

    df['health_score'] = (
        (df['strategic_footprint'] * 0.2) + 
        (df['systemic_resilience'] * 0.3) + 
        (df['innovation_acceleration'] * 0.3) + 
        (df['expansion_momentum'] * 0.2)
    )

    def get_action(score):
        if score >= 0.75: return "NEURAL CORE"
        elif 0.50 <= score < 0.75: return "STABLE OPERATIONS"
        elif 0.30 <= score < 0.50: return "MONITOR FRICTION"
        else: return "URGENT INTERVENTION"

    df['strategic_action'] = df['health_score'].apply(get_action)
    return df

if __name__ == "__main__":
    # Robust absolute pathing
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.abspath(os.path.join(script_dir, '..', 'calgary_business_data.csv'))
    output_path = os.path.abspath(os.path.join(script_dir, '..', 'nexus_intelligence_feed.csv'))

    if os.path.exists(input_path):
        raw_df = pd.read_csv(input_path)
        processed_df = calculate_nexus_metrics(raw_df)
        processed_df.to_csv(output_path, index=False)
        print(f"Process Complete. Output saved to: {output_path}")
    else:
        print(f"CRITICAL ERROR: File not found at {input_path}")
        # This will trigger the Exit Code 1 you saw
        exit(1)
