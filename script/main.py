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

def run_pipeline():
    output_dir = 'data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    headers = {'User-Agent': 'Nexus-Command-Center/1.0'}

    # 1. DOWNLOAD SPATIAL BOUNDARIES
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

    # 3. ATOMIC TRANSFORMATIONS
    df.columns = [c.replace('_', '').lower() for c in df.columns]
    df['comdistnm'] = df['comdistnm'].fillna('Unknown')
    df['industry_sector'] = df['licencetypes'].fillna('UNKNOWN').apply(get_industry_sector)
    
    # Growth Velocity Logic (Last 12 Months)
    df['first_iss_dt'] = pd.to_datetime(df['first_iss_dt'], errors='coerce')
    one_year_ago = datetime.now() - timedelta(days=365)
    df['is_growth'] = (df['first_iss_dt'] >= one_year_ago).astype(int)

    # 4. KPI CALCULATIONS
    # Community Volume & Impact Weight
    comm_stats = df.groupby('comdistnm').size().rename('community_volume')
    df = df.merge(comm_stats, on='comdistnm', how='left')
    city_avg = comm_stats.mean()
    df['impact_weight'] = (df['community_volume'] / city_avg).round(2)

    # Vitality Index (Licensed Rate)
    df['is_licensed'] = df['jobstatusdesc'].apply(lambda x: 1 if x == 'Licensed' else 0)
    vitality = df.groupby('comdistnm')['is_licensed'].mean().round(2).rename('vitality_index')
    df = df.merge(vitality, on='comdistnm', how='left')

    # Strategic Action Logic
    def get_action(weight):
        if weight > 1.5: return "URGENT INTERVENTION"
        elif weight > 1.0: return "MONITOR FRICTION"
        return "STABLE OPERATIONS"
    df['strategic_action'] = df['impact_weight'].apply(get_action)

    # Sector Growth Velocity
    sector_growth = df.groupby('industry_sector')['is_growth'].sum().rename('recent_growth_count')
    df = df.merge(sector_growth, on='industry_sector', how='left')

    # 5. EXPORT
    kpi_file = os.path.join(output_dir, 'calgary_strategy_kpis.csv')
    df.to_csv(kpi_file, index=False, quoting=1)
    print("Nexus Update Complete: Data/Spatial Files Synced.")

if __name__ == "__main__":
    run_pipeline()
