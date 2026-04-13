import pandas as pd
import requests
import os
import sys

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
    dataset_id = "vdjc-pybd" 
    url = f"https://data.calgary.ca/resource/{dataset_id}.json"
    
    # FIXED: Using 'jobstatusdesc' instead of 'licencestatus'
    # FIXED: Using Calgary's specific status values
    params = {
        "$select": "comdistnm, jobstatusdesc, licencetypes",
        "$where": "jobstatusdesc IN ('Licensed', 'Pending Renewal', 'Renewal Invoiced', 'Renewal Licensed')",
        "$limit": 100000
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        print(f"Connecting to Calgary Data Hub...")
        r = requests.get(url, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        print(f"Data successfully retrieved: {len(df)} rows.")
    except Exception as e:
        print(f"CRITICAL EXTRACTION ERROR: {e}")
        # Print the response body to debug the 400 error further if it persists
        if 'r' in locals():
            print(f"Response Body: {r.text}")
        sys.exit(1)

    # 3. ATOMIC TO AGGREGATE PROCESSING
    df['industry_sector'] = df['licencetypes'].fillna('UNKNOWN').apply(get_industry_sector)

    # Status Logic: "Active" vs "Churn-Risk"
    # Calgary doesn't show historic 'Cancelled' in the current active view, 
    # so we track 'Invoiced' and 'Pending' as potential churn/friction points.
    nexus = df.groupby(['comdistnm', 'industry_sector']).agg(
        active_licenses=('jobstatusdesc', lambda x: (x.str.contains('Licensed', case=False)).sum()),
        admin_friction=('jobstatusdesc', lambda x: (x.str.contains('Invoiced|Pending', case=False)).sum()),
        total_volume=('jobstatusdesc', 'count')
    ).reset_index()

    # 4. STRATEGIC METRICS
    nexus['vitality_index'] = ((nexus['active_licenses'] / nexus['total_volume']) * 100).round(2)
    avg_vol = nexus['total_volume'].mean()
    nexus['impact_weight'] = (nexus['total_volume'] / avg_vol).round(2)
    
    # 5. RECOMMENDATION ENGINE
    def get_action(row):
        if row['vitality_index'] < 70 and row['impact_weight'] > 1.5:
            return "URGENT INTERVENTION: High admin friction in critical economic hub."
        elif row['vitality_index'] > 90 and row['impact_weight'] > 2.0:
            return "STRATEGIC ASSET: High-performing anchor sector."
        else:
            return "STABLE: Standard maintenance of operations."

    nexus['strategic_action'] = nexus.apply
