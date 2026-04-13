import pandas as pd
import requests
import os
import sys
from datetime import datetime, timedelta

def get_industry_sector(lt):
    """Dynamic Extraction Logic: Prevents 'Diversified/Other' accumulation."""
    if not lt or lt == "UNKNOWN":
        return "Diversified/Other"
    
    lt = str(lt).upper()
    logic_gate = {
        'Food & Beverage': ['FOOD', 'RESTAURANT', 'DINING', 'CAFE', 'BAKERY'],
        'Liquor & Nightlife': ['LIQUOR', 'DRINKING', 'PUB', 'CLUB', 'BAR'],
        'Retail Trade': ['RETAIL', 'STORE', 'SHOP', 'MARKET', 'DEALER'],
        'Construction & Trades': ['CONTRACTOR', 'CONSTRUCTION', 'ELECTRICAL', 'PLUMBING'],
        'Professional Services': ['OFFICE', 'CONSULTING', 'LEGAL', 'ACCOUNTING'],
        'Health & Medical': ['HEALTH', 'MEDICAL', 'CLINIC', 'DENTAL', 'PHARMACY'],
        'Cannabis': ['CANNABIS', 'MARIJUANA'],
        'Personal Services': ['SALON', 'BARBER', 'SPA', 'CLEANING'],
        'Tech & Media': ['SOFTWARE', 'COMPUTER', 'TECHNOLOGY', 'MEDIA'],
        'Industrial & Energy': ['ENERGY', 'OIL', 'GAS', 'MANUFACTURING'],
        'Transportation': ['TAXI', 'LIMO', 'DELIVERY', 'COURIER', 'TRUCKING'],
        'Automotive': ['MOTOR VEHICLE', 'AUTO', 'REPAIR', 'GARAGE']
    }

    for sector, keywords in logic_gate.items():
        if any(kw in lt for kw in keywords):
            return sector
            
    parts = lt.replace('-', ' ').replace('(', ' ').split()
    if parts:
        noise = ['THE', 'AND', 'OF', 'FOR', 'LIMITED', 'SERVICE', 'SERVICES', 'LICENSE', 'TIER']
        clean_parts = [p for p in parts if p not in noise]
        return clean_parts[0].capitalize() if clean_parts else "Diversified/Other"
    
    return "Diversified/Other"

def get_action(row):
    """Balanced Tri-Factor Risk Model."""
    if (row['impact_weight'] > 2.0 and row['vitality_index'] < 0.70):
        return "URGENT INTERVENTION"
    elif (row['impact_weight'] > 1.2 and row['vitality_index'] < 0.85):
        return "MONITOR FRICTION"
    else:
        return "STABLE OPERATIONS"

def run_pipeline():
    output_dir = 'data'
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    # UPDATED: Removed 'firstissdt' to stop the API Query Coordinator Error
    url = "https://data.calgary.ca/resource/vdjc-pybd.json"
    params = {
        "$select": "comdistnm, jobstatusdesc, licencetypes, tradename, first_iss_dt",
        "$where": "jobstatusdesc IN ('Licensed', 'Pending Renewal', 'Renewal Invoiced', 'Renewal Licensed')",
        "$limit": 100000
    }

    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status() # Check for HTTP errors
        data = r.json()
        
        # Check if the API returned an error message in JSON format
        if isinstance(data, dict) and 'message' in data:
            print(f"API Error: {data['message']}")
            sys.exit(1)
            
        df = pd.DataFrame(data)
    except Exception as e:
        print(f"Critical Sync Failure: {e}")
        sys.exit(1)

    # Processing
    df['comdistnm'] = df['comdistnm'].fillna('Unknown')
    df['industry_sector'] = df['licencetypes'].apply(get_industry_sector)
    
    # Date Handling
    df['first_iss_dt'] = pd.to_datetime(df['first_iss_dt'], errors='coerce').dt.tz_localize(None)
    one_year_ago = datetime.now() - timedelta(days=365)
    df['is_growth'] = (df['first_iss_dt'] >= one_year_ago).astype(int)

    # KPIs
    comm_stats = df.groupby('comdistnm').size().rename('community_volume')
    df = df.merge(comm_stats, on='comdistnm', how='left')
    df['impact_weight'] = (df['community_volume'] / comm_stats.mean()).round(2)

    df['is_licensed'] = (df['jobstatusdesc'] == 'Licensed').astype(int)
    vitality = df.groupby('comdistnm')['is_licensed'].mean().round(2).rename('vitality_index')
    df = df.merge(vitality, on='comdistnm', how='left')

    sector_growth = df.groupby('industry_sector')['is_growth'].sum().rename('recent_growth_count')
    df = df.merge(sector_growth, on='industry_sector', how='left')

    df['strategic_action'] = df.apply(get_action, axis=1)

    # Save
    df.to_csv(os.path.join(output_dir, 'calgary_strategy_kpis.csv'), index=False)
    print("Intelligence Pulse Restored.")

if __name__ == "__main__":
    run_pipeline()
