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
    df.columns = [c.lower() for c in df.columns]
    
    # 1. KPI Normalization
    df['industry_sector'] = df['licencetypes'].apply(categorize_sector)
    df['strategic_footprint'] = df['impact_weight'] / df['impact_weight'].max()
    df['systemic_resilience'] = df['vitality_index'] / df['vitality_index'].max()
    df['expansion_momentum'] = df['growth_count'] / df['growth_count'].max()

    # 2. Innovation Acceleration
    city_avg_growth = df['growth_count'].mean()
    df['innovation_acceleration'] = (df['growth_count'] / (city_avg_growth if city_avg_growth != 0 else 1)) * df['systemic_resilience']
    df['innovation_acceleration'] = df['innovation_acceleration'].clip(upper=2.0)

    # 3. Integrated Health Score
    df['health_score'] = (
        (df['strategic_footprint'] * 0.2) + 
        (df['systemic_resilience'] * 0.3) + 
        (df['innovation_acceleration'] * 0.3) + 
        (df['expansion_momentum'] * 0.2)
    )

    # 4. Strategic Action Labels
    def get_action(score):
        if score >= 0.75: return "NEURAL CORE"
        elif 0.50 <= score < 0.75: return "STABLE OPERATIONS"
        elif 0.30 <= score < 0.50: return "MONITOR FRICTION"
        else: return "URGENT INTERVENTION"

    df['strategic_action'] = df['health_score'].apply(get_action)
    return df

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.abspath(os.path.join(script_dir, '..'))
    
    # INPUT: Now in the Root folder
    input_file = os.path.join(root_dir, 'calgary_strategy_kpis.csv')
    
    # OUTPUT: Still in the Data folder (Script will create folder if missing)
    data_dir = os.path.join(root_dir, 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    output_file = os.path.join(data_dir, 'nexus_intelligence_feed.csv')

    if os.path.exists(input_file):
        try:
            raw_df = pd.read_csv(input_file)
            if raw_df.empty:
                print("Error: Input file is empty.")
                exit(1)
            
            processed_df = calculate_nexus_metrics(raw_df)
            processed_df.to_csv(output_file, index=False)
            print(f"Success. Intelligence Feed generated at: {output_file}")
            
        except Exception as e:
            print(f"Processing Error: {e}")
            exit(1)
    else:
        print(f"Critical Error: {input_file} not found in root.")
        exit(1)
