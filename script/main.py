import pandas as pd
import requests
import os
import sys
import json
from datetime import datetime, timedelta

def get_industry_sector(lt):
    """30-Category High-Resolution Mapping."""
    lt = str(lt).upper()
    mapping = {
        'Software & SaaS': ['SOFTWARE', 'SAAS', 'DEVELOPMENT'],
        'Data & AI Services': ['DATA', 'ANALYTICS', 'INTELLIGENCE', 'AI'],
        'Cybersecurity': ['SECURITY', 'CYBER'],
        'Hardware & Robotics': ['HARDWARE', 'ROBOTICS', 'ELECTRONIC'],
        'Fine Dining': ['RESTAURANT', 'DINING'],
        'Fast Casual': ['FAST FOOD', 'TAKE OUT'],
        'Breweries & Distilleries': ['BREWERY', 'DISTILLERY', 'CRAFT BEER'],
        'Cafes & Bakeries': ['CAFE', 'COFFEE', 'BAKERY'],
        'Nightlife & Bars': ['BAR', 'NIGHTCLUB', 'LOUNGE'],
        'Specialized Medicine': ['CLINIC', 'SPECIALIST', 'SURGERY'],
        'Mental Health Services': ['PSYCHOLOGY', 'COUNSELLING', 'THERAPY'],
        'Pharmaceuticals': ['PHARMACY', 'DRUG STORE'],
        'Fitness & Gyms': ['FITNESS', 'GYM', 'YOGA', 'STUDIO'],
        'Renewable Energy': ['SOLAR', 'WIND', 'RENEWABLE'],
        'Oil & Gas Services': ['ENERGY', 'OIL', 'GAS', 'PETROLEUM'],
        'Electrical Engineering': ['ELECTRICAL', 'ELECTRICIAN'],
        'Civil Construction': ['CIVIL', 'INFRASTRUCTURE'],
        'Residential Trades': ['PLUMBING', 'ROOFING', 'HEATING'],
        'Luxury Goods': ['JEWELLERY', 'BOUTIQUE', 'LUXURY'],
        'Automotive Sales': ['DEALERSHIP', 'CAR SALES'],
        'E-commerce Logistics': ['DELIVERY', 'COURIER', 'LOGISTICS'],
        'General Merchandise': ['RETAIL', 'STORE', 'SHOP'],
        'Legal Services': ['LEGAL', 'LAWYER', 'ATTORNEY'],
        'Accounting & Tax': ['ACCOUNTANT', 'TAX', 'BOOKKEEPING'],
        'Real Estate Services': ['REAL ESTATE', 'REALTOR', 'BROKERAGE'],
        'Marketing & Media': ['MARKETING', 'ADVERTISING', 'DESIGN'],
        'Cannabis Retail': ['CANNABIS', 'MARIJUANA'],
        'Liquor Retail': ['LIQUOR', 'WINE', 'SPIRITS'],
        'Education & Training': ['SCHOOL', 'TUTOR', 'TRAINING'],
        'Personal Grooming': ['HAIR', 'SALON', 'BARBER', 'SPA']
    }
    for sector, keywords in mapping.items():
        if any(kw in lt for kw in keywords):
            return sector
    return 'Diversified/Other'

def get_action(row):
    """Refined Tri-Factor Risk Model."""
    weight = row['impact_weight']
    vitality = row['vitality_index']
    growth = row['recent_growth_count']
    
    if (weight > 2.0 and vitality < 0.70):
        return "URGENT INTERVENTION"
    elif (weight > 1.2 and vitality < 0.85) or (growth > 25 and vitality < 0.90):
        return "MONITOR FRICTION"
    else:
        return "STABLE OPERATIONS"

def run_pipeline():
    output_dir = 'data'
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    headers = {'User-Agent': 'Nexus-Stark-Engine/2.0'}
    url = "https://data.calgary.ca/resource/vdjc-pybd.json"
    
    # Selecting the most likely API field names
    params = {
        "$select": "comdistnm, jobstatusdesc, licencetypes, tradename, first_iss_dt, firstissdt",
        "$where": "jobstatusdesc IN ('Licensed', 'Pending Renewal', 'Renewal Invoiced', 'Renewal Licensed')",
        "$limit": 100000
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=60)
        df = pd.DataFrame(r.json())
    except Exception as e:
        print(f"Extraction Error: {e}"); sys.exit(1)

    # DYNAMIC FIELD ALIGNMENT: Handles API shifts between first_iss_dt and firstissdt
    if 'firstissdt' in df.columns and 'first_iss_dt' not in df.columns:
        df.rename(columns={'firstissdt': 'first_iss_dt'}, inplace=True)
    
    # Ensure all required columns exist to prevent KeyError
    required_cols = ['comdistnm', 'jobstatusdesc', 'licencetypes', 'first_iss_dt']
    for col in required_cols:
        if col not in df.columns:
            df[col] = "UNKNOWN"

    # Transformation
    df['comdistnm'] = df['comdistnm'].fillna('Unknown')
    df['industry_sector'] = df['licencetypes'].apply(get_industry_sector)
    
    # Timezone-Naive Fix for GitHub Actions
    df['first_iss_dt'] = pd.to_datetime(df['first_iss_dt'], errors='coerce').dt.tz_localize(None)
    one_year_ago = datetime.now() - timedelta(days=365)
    df['is_growth'] = (df['first_iss_dt'] >= one_year_ago).astype(int)

    # KPI Calculations
    comm_stats = df.groupby('comdistnm').size().rename('community_volume')
    df = df.merge(comm_stats, on='comdistnm', how='left')
    df['impact_weight'] = (df['community_volume'] / comm_stats.mean()).round(2)

    df['is_licensed'] = df['jobstatusdesc'].apply(lambda x: 1 if x == 'Licensed' else 0)
    vitality = df.groupby('comdistnm')['is_licensed'].mean().round(2).rename('vitality_index')
    df = df.merge(vitality, on='comdistnm', how='left')

    sector_growth = df.groupby('industry_sector')['is_growth'].sum().rename('recent_growth_count')
    df = df.merge(sector_growth, on='industry_sector', how='left')

    # Final Strategic Action
    df['strategic_action'] = df.apply(get_action, axis=1)

    # Exporting
    df.to_csv(os.path.join(output_dir, 'calgary_strategy_kpis.csv'), index=False)
    print(f"Sync Success. Processed {len(df)} records.")

if __name__ == "__main__":
    run_pipeline()
