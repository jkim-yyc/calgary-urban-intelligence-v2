import pandas as pd
import requests
import os
import sys
import json
from datetime import datetime, timedelta

def get_industry_sector(lt):
    """Expanded 30-Category High-Resolution Mapping."""
    lt = str(lt).upper()
    mapping = {
        # TECH & INTELLIGENCE
        'Software & SaaS': ['SOFTWARE', 'SAAS', 'DEVELOPMENT'],
        'Data & AI Services': ['DATA', 'ANALYTICS', 'INTELLIGENCE', 'AI'],
        'Cybersecurity': ['SECURITY', 'CYBER'],
        'Hardware & Robotics': ['HARDWARE', 'ROBOTICS', 'ELECTRONIC'],
        # FOOD & BEVERAGE
        'Fine Dining': ['RESTAURANT', 'DINING'],
        'Fast Casual': ['FAST FOOD', 'TAKE OUT'],
        'Breweries & Distilleries': ['BREWERY', 'DISTILLERY', 'CRAFT BEER'],
        'Cafes & Bakeries': ['CAFE', 'COFFEE', 'BAKERY'],
        'Nightlife & Bars': ['BAR', 'NIGHTCLUB', 'LOUNGE'],
        # MEDICAL & HEALTH
        'Specialized Medicine': ['CLINIC', 'SPECIALIST', 'SURGERY'],
        'Mental Health Services': ['PSYCHOLOGY', 'COUNSELLING', 'THERAPY'],
        'Pharmaceuticals': ['PHARMACY', 'DRUG STORE'],
        'Fitness & Gyms': ['FITNESS', 'GYM', 'YOGA', 'STUDIO'],
        # TRADES & INDUSTRIAL
        'Renewable Energy': ['SOLAR', 'WIND', 'RENEWABLE'],
        'Oil & Gas Services': ['ENERGY', 'OIL', 'GAS', 'PETROLEUM'],
        'Electrical Engineering': ['ELECTRICAL', 'ELECTRICIAN'],
        'Civil Construction': ['CIVIL', 'INFRASTRUCTURE'],
        'Residential Trades': ['PLUMBING', 'ROOFING', 'HEATING'],
        # RETAIL
        'Luxury Goods': ['JEWELLERY', 'BOUTIQUE', 'LUXURY'],
        'Automotive Sales': ['DEALERSHIP', 'CAR SALES'],
        'E-commerce Logistics': ['DELIVERY', 'COURIER', 'LOGISTICS'],
        'General Merchandise': ['RETAIL', 'STORE', 'SHOP'],
        # PROFESSIONAL SERVICES
        'Legal Services': ['LEGAL', 'LAWYER', 'ATTORNEY'],
        'Accounting & Tax': ['ACCOUNTANT', 'TAX', 'BOOKKEEPING'],
        'Real Estate Services': ['REAL ESTATE', 'REALTOR', 'BROKERAGE'],
        'Marketing & Media': ['MARKETING', 'ADVERTISING', 'DESIGN'],
        # SPECIALIZED
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
    """Refined Tri-Factor Risk Model for Balanced Distribution."""
    weight = row['impact_weight']
    vitality = row['vitality_index']
    growth = row['recent_growth_count']
    
    # URGENT: High Impact (>2.0) + Critical Vitality (<0.70)
    if (weight > 2.0 and vitality < 0.70):
        return "URGENT INTERVENTION"
    # MONITOR: Moderate friction or high-velocity growth hotspots
    elif (weight > 1.2 and vitality < 0.85) or (growth > 25 and vitality < 0.90):
        return "MONITOR FRICTION"
    # STABLE: Default high-performance state
    else:
        return "STABLE OPERATIONS"

def run_pipeline():
    output_dir = 'data'
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    headers = {'User-Agent': 'Nexus-Stark-Engine/2.0'}
    url = "https://data.calgary.ca/resource/vdjc-pybd.json"
    params = {
        "$select": "comdistnm, jobstatusdesc, licencetypes, tradename, first_iss_dt",
        "$where": "jobstatusdesc IN ('Licensed', 'Pending Renewal', 'Renewal Invoiced', 'Renewal Licensed')",
        "$limit": 100000
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=60)
        df = pd.DataFrame(r.json())
        # Standardize column names
        df.columns = [c.replace('_', '').lower() for c in df.columns]
    except Exception as e:
        print(f"Error: {e}"); sys.exit(1)

    # 1. Processing
    df['comdistnm'] = df['comdistnm'].fillna('Unknown')
    df['industry_sector'] = df['licencetypes'].fillna('UNKNOWN').apply(get_industry_sector)
    
    # 2. Timezone-Naive Conversion for Velocity
    df['firstissdt'] = pd.to_datetime(df['firstissdt'], errors='coerce').dt.tz_localize(None)
    one_year_ago = datetime.now() - timedelta(days=365)
    df['is_growth'] = (df['firstissdt'] >= one_year_ago).astype(int)

    # 3. KPI Calculations
    comm_stats = df.groupby('comdistnm').size().rename('community_volume')
    df = df.merge(comm_stats, on='comdistnm', how='left')
    df['impact_weight'] = (df['community_volume'] / comm_stats.mean()).round(2)

    df['is_licensed'] = df['jobstatusdesc'].apply(lambda x: 1 if x == 'Licensed' else 0)
    vitality = df.groupby('comdistnm')['is_licensed'].mean().round(2).rename('vitality_index')
    df = df.merge(vitality, on='comdistnm', how='left')

    sector_growth = df.groupby('industry_sector')['is_growth'].sum().rename('recent_growth_count')
    df = df.merge(sector_growth, on='industry_sector', how='left')

    # 4. Action Logic
    df['strategic_action'] = df.apply(get_action, axis=1)

    # 5. Export
    df.to_csv(os.path.join(output_dir, 'calgary_strategy_kpis.csv'), index=False)
    print("Intelligence Sync Complete.")

if __name__ == "__main__":
    run_pipeline()
