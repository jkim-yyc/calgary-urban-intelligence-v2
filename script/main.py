import pandas as pd
import requests
import os
import sys
import json
from datetime import datetime, timedelta

def get_industry_sector(lt):
    """Categorizes raw license types into strategic economic sectors."""
    lt = str(lt).upper()
    mapping = {
        'Food & Beverage': ['FOOD', 'RESTAURANT', 'DINING', 'CAFE', 'CATERING', 'BAKERY'],
        'Retail - General': ['RETAIL', 'DEALER', 'STORE', 'SHOP', 'VARIETY'],
        'Health & Wellness': ['HEALTH', 'MEDICAL', 'CLINIC', 'PHARMACY', 'DENTAL', 'VET'],
        'Personal Services': ['BEAUTY', 'HAIR', 'SALON', 'BARBER', 'SPA', 'TATTOO', 'LAUNDRY'],
        'Construction & Trades': ['CONTRACTOR', 'CONSTRUCTION', 'PLUMBING', 'ELECTRICAL', 'ROOFING'],
        'Automotive Services': ['AUTO', 'VEHICLE', 'CAR WASH', 'MECHANIC', 'TIRE', 'GARAGE'],
        'Professional Services': ['CONSULTING', 'ENGINEER', 'ARCHITECT', 'ACCOUNTANT', 'LEGAL'],
        'Cannabis & Liquor': ['CANNABIS', 'LIQUOR', 'BREWERY', 'DISTILLERY', 'ALCOHOL'],
        'Information Technology': ['SOFTWARE', 'COMPUTER', 'IT SERVICES', 'TECHNOLOGY']
    }
    for sector, keywords in mapping.items():
        if any(kw in lt for kw in keywords):
            return sector
    return 'Other/Diversified'

def get_action(row):
    """Tri-Factor Risk Model: Prioritizes based on Impact, Vitality, and Velocity."""
    weight = row['impact_weight']
    vitality = row['vitality_index']
    growth = row['recent_growth_count']
    
    # URGENT: High Impact + High Friction OR High Impact + Explosive Growth causing Friction
    if (weight > 1.5 and vitality < 0.80) or (weight > 1.2 and growth > 20 and vitality < 0.85):
        return "URGENT INTERVENTION"
    # MONITOR: High growth hotspots or moderate impact areas with dipping health
    elif growth > 15 or vitality < 0.90 or weight > 1.0:
        return "MONITOR FRICTION"
    # STABLE: High vitality and manageable growth
    else:
        return "STABLE OPERATIONS"

def run_pipeline():
    output_dir = 'data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    headers = {'User-Agent': 'Nexus-Strategy-Engine/1.0'}

    # 1. DOWNLOAD SPATIAL DATA
    spatial_url = "https://data.calgary.ca/resource/surr-xmvs.geojson"
    try:
        resp_geo = requests.get(spatial_url, headers=headers, timeout=60)
        with open(os.path.join(output_dir, 'calgary_boundaries.geojson'), 'w') as f:
            json.dump(resp_geo.json(), f)
    except Exception as e:
        print(f"Spatial Error: {e}")

    # 2. DOWNLOAD LICENSE DATA
    url = "https://data.calgary.ca/resource/vdjc-pybd.json"
    params = {
        "$select": "comdistnm, jobstatusdesc, licencetypes, tradename, address, first_iss_dt",
        "$where": "jobstatusdesc IN ('Licensed', 'Pending Renewal', 'Renewal Invoiced', 'Renewal Licensed')",
        "$limit": 100000
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=60)
        df = pd.DataFrame(r.json())
    except Exception as e:
        print(f"Extraction Error: {e}")
        sys.exit(1)

    # 3. CORE TRANSFORMATIONS
    # Standardize column names (remove underscores and lowercase)
    df.columns = [c.replace('_', '').lower() for c in df.columns]
    df['comdistnm'] = df['comdistnm'].fillna('Unknown')
    df['industry_sector'] = df['licencetypes'].fillna('UNKNOWN').apply(get_industry_sector)
    
    # 4. TIMEZONE-AWARE GROWTH CALCULATION (Velocity)
    # Convert and strip timezone to allow comparison with datetime.now()
    df['firstissdt'] = pd.to_datetime(df['firstissdt'], errors='coerce').dt.tz_localize(None)
    one_year_ago = datetime.now() - timedelta(days=365)
    df['is_growth'] = (df['firstissdt'] >= one_year_ago).astype(int)

    # 5. KPI CALCULATIONS (Tri-Factor)
    # Impact Weight (Relative to City Mean)
    comm_stats = df.groupby('comdistnm').size().rename('community_volume')
    df = df.merge(comm_stats, on='comdistnm', how='left')
    city_avg = comm_stats.mean()
    df['impact_weight'] = (df['community_volume'] / city_avg).round(2)

    # Vitality Index (Licensed Rate)
    df['is_licensed'] = df['jobstatusdesc'].apply(lambda x: 1 if x == 'Licensed' else 0)
    vitality = df.groupby('comdistnm')['is_licensed'].mean().round(2).rename('vitality_index')
    df = df.merge(vitality, on='comdistnm', how='left')

    # Sector Velocity (Total growth count available per row)
    sector_growth = df.groupby('industry_sector')['is_growth'].sum().rename('recent_growth_count')
    df = df.merge(sector_growth, on='industry_sector', how='left')

    # 6. STRATEGIC ACTION (Decision Logic)
    df['strategic_action'] = df.apply(get_action, axis=1)

    # 7. EXPORT
    df.to_csv(os.path.join(output_dir, 'calgary_strategy_kpis.csv'), index=False, quoting=1)
    print("Strategy Update Success: Data Synced to /data.")

if __name__ == "__main__":
    run_pipeline()
