import pandas as pd
import requests
import os
import sys
import json

def get_industry_sector(lt):
    """Categorizes raw license types into 30 strategic economic sectors."""
    lt = str(lt).upper()
    mapping = {
        'Food & Beverage': ['FOOD', 'RESTAURANT', 'DINING', 'CAFE', 'CATERING', 'BAKERY'],
        'Retail - General': ['RETAIL', 'DEALER', 'STORE', 'SHOP', 'VARIETY'],
        'Health & Wellness': ['HEALTH', 'MEDICAL', 'CLINIC', 'PHARMACY', 'DENTAL', 'VET'],
        'Personal Services': ['BEAUTY', 'HAIR', 'SALON', 'BARBER', 'SPA', 'TATTOO', 'LAUNDRY'],
        'Construction & Trades': ['CONTRACTOR', 'CONSTRUCTION', 'PLUMBING', 'ELECTRICAL', 'ROOFING'],
        'Automotive Services': ['AUTO', 'VEHICLE', 'CAR WASH', 'MECHANIC', 'TIRE', 'GARAGE'],
        'Professional Services': ['CONSULTING', 'ENGINEER', 'ARCHITECT', 'ACCOUNTANT', 'LEGAL'],
        'Education & Instruction': ['SCHOOL', 'EDUCATION', 'TUTOR', 'TRAINING', 'ACADEMY', 'YOGA', 'DANCE'],
        'Cannabis & Liquor': ['CANNABIS', 'LIQUOR', 'BREWERY', 'DISTILLERY', 'ALCOHOL'],
        'Financial Services': ['FINANCIAL', 'BANK', 'LENDING', 'PAWN', 'INVESTMENT'],
        'Logistics & Transport': ['TRANSPORT', 'TRUCKING', 'COURIER', 'DELIVERY', 'WAREHOUSE', 'TAXICAB'],
        'Real Estate & Housing': ['REAL ESTATE', 'PROPERTY', 'LANDLORD', 'APARTMENT', 'LEASING'],
        'Hospitality & Tourism': ['HOTEL', 'MOTEL', 'LODGING', 'TOURISM', 'TRAVEL', 'BED & BREAKFAST'],
        'Entertainment & Arts': ['ENTERTAINMENT', 'THEATRE', 'CINEMA', 'ARTS', 'GALLERY', 'MUSEUM'],
        'Manufacturing & Industrial': ['MANUFACTURING', 'FACTORY', 'PRODUCTION', 'INDUSTRIAL', 'MACHINE'],
        'Pet & Animal Services': ['PET', 'DOG', 'ANIMAL', 'KENNEL', 'GROOMING'],
        'Childcare Services': ['DAY CARE', 'CHILD CARE', 'PRESCHOOL'],
        'Security & Investigation': ['SECURITY', 'INVESTIGATION', 'ALARM', 'GUARD'],
        'Information Technology': ['SOFTWARE', 'COMPUTER', 'IT SERVICES', 'TECHNOLOGY'],
        'Wholesale Trade': ['WHOLESALE', 'DISTRIBUTION', 'IMPORT', 'EXPORT'],
        'Waste & Environment': ['WASTE', 'RECYCLING', 'ENVIRONMENTAL', 'CLEANING'],
        'Fitness & Recreation': ['FITNESS', 'GYM', 'RECREATION', 'SPORTS', 'CLUB'],
        'Media & Communication': ['MEDIA', 'PUBLISHING', 'ADVERTISING', 'MARKETING', 'PRINTING'],
        'Energy & Utilities': ['ENERGY', 'OIL', 'GAS', 'UTILITY', 'SOLAR', 'POWER'],
        'Non-Profit & Social': ['CHARITY', 'NON-PROFIT', 'ASSOCIATION', 'SOCIAL SERVICE'],
        'Agriculture': ['FARM', 'AGRICULTURE', 'GREENHOUSE', 'LIVESTOCK'],
        'Religious Services': ['CHURCH', 'RELIGIOUS', 'TEMPLE', 'MOSQUE'],
        'Special Events': ['EVENT', 'FESTIVAL', 'MARKET', 'POP-UP'],
        'Massage & Bodywork': ['MASSAGE', 'BODYWORK', 'REFLEXOLOGY'],
        'Home Occupation': ['HOME OCCUPATION']
    }
    for sector, keywords in mapping.items():
        if any(kw in lt for kw in keywords):
            return sector
    return 'Other/Diversified'

def run_pipeline():
    # 1. SETUP PATHS
    output_dir = 'data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    headers = {'User-Agent': 'Mozilla/5.0'}

    # 2. DOWNLOAD SPATIAL BOUNDARIES (Community Map)
    spatial_id = "surr-xmvs" # Community District Boundaries
    spatial_url = f"https://data.calgary.ca/resource/{spatial_id}.geojson"
    
    print("Nexus Step 1: Downloading Spatial Boundaries...")
    try:
        resp_geo = requests.get(spatial_url, headers=headers, timeout=60)
        resp_geo.raise_for_status()
        with open(os.path.join(output_dir, 'calgary_boundaries.geojson'), 'w') as f:
            json.dump(resp_geo.json(), f)
        print("Success: Spatial file saved.")
    except Exception as e:
        print(f"Spatial Error: {e}")

    # 3. DOWNLOAD & PROCESS KPI DATA
    dataset_id = "vdjc-pybd" 
    url = f"https://data.calgary.ca/resource/{dataset_id}.json"
    
    params = {
        "$select": "comdistnm, jobstatusdesc, licencetypes, tradename, address, first_iss_dt",
        "$where": "jobstatusdesc IN ('Licensed', 'Pending Renewal', 'Renewal Invoiced', 'Renewal Licensed')",
        "$limit": 100000
    }

    print("Nexus Step 2: Extracting License Data...")
    try:
        r = requests.get(url, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
    except Exception as e:
        print(f"Extraction Error: {e}")
        sys.exit(1)

    df.columns = [c.replace('_', '').lower() for c in df.columns]
    df['industry_sector'] = df['licencetypes'].fillna('UNKNOWN').apply(get_industry_sector)

    # Calculate Community Context
    comm_stats = df.groupby('comdistnm').size().rename('community_volume')
    df = df.merge(comm_stats, on='comdistnm', how='left')
    
    city_avg = comm_stats.mean()
    df['impact_weight'] = (df['community_volume'] / city_avg).round(2)

    def get_action(row):
        if row['impact_weight'] > 1.5: return "URGENT INTERVENTION: High-impact hub."
        elif row['impact_weight'] > 1.0: return "MONITOR: Moderate significance."
        else: return "STABLE: Localized cluster."

    df['strategic_action'] = df.apply(get_action, axis=1)

    # Save KPI Dataset
    kpi_file = os.path.join(output_dir, 'calgary_strategy_kpis.csv')
    df.to_csv(kpi_file, index=False)
    print(f"Deployment Successful: All Nexus files updated in /{output_dir}")

if __name__ == "__main__":
    run_pipeline()
