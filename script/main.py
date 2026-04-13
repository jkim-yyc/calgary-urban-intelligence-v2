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
    # 1. SMART EXTRACTION: Use the Master License ID and filter at source
    # This ID represents the primary City of Calgary Business License view
    dataset_id = "vdjc-pybd" 
    url = f"https://data.calgary.ca/resource/{dataset_id}.json"
    
    # Optimization: Only pull necessary columns and statuses to stay under rate limits
    params = {
        "$select": "comdistnm, licencestatus, licencetypes",
        "$where": "licencestatus IN ('Issued', 'Cancelled', 'Expired')",
        "$limit": 100000
    }
    
    # Security: Mask request as a standard browser to avoid 403 Forbidden errors
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        print(f"Connecting to Calgary Data Hub...")
        r = requests.get(url, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        print("Data successfully retrieved.")
    except Exception as e:
        print(f"CRITICAL EXTRACTION ERROR: {e}")
        sys.exit(1)

    # 2. COLUMN ALIGNMENT
    # Ensure column names are clean and findable
    df.columns = [c.replace('_', '').lower() for c in df.columns]
    
    # 3. ATOMIC TO AGGREGATE PROCESSING
    df['industry_sector'] = df['licencetypes'].apply(get_industry_sector)

    # Group by Community and Sector to find the 'Intersection'
    nexus = df.groupby(['comdistnm', 'industry_sector']).agg(
        active_licenses=('licencestatus', lambda x: (x.str.contains('Issued', case=False)).sum()),
        churn_events=('licencestatus', lambda x: (x.str.contains('Cancelled|Expired', case=False)).sum()),
        total_volume=('licencestatus', 'count')
    ).reset_index()

    # 4. STRATEGIC METRICS
    nexus['churn_rate'] = (nexus['churn_events'] / nexus['total_volume']).fillna(0)
    nexus['vitality_index'] = ((nexus['active_licenses'] / nexus['total_volume']) * 100).round(2)
    
    # Impact Weighting: Scales the community sector relative to the city average
    avg_vol = nexus['total_volume'].mean()
    nexus['impact_weight'] = (nexus['total_volume'] / avg_vol).round(2)
    
    # 5. RECOMMENDATION ENGINE (Prescriptive Action)
    def get_action(row):
        if row['churn_rate'] > 0.35 and row['impact_weight'] > 1.5:
            return "URGENT INTERVENTION: High systemic risk in critical economic hub."
        elif row['churn_rate'] > 0.35:
            return "MONITOR: Elevated churn detected in localized cluster."
        elif row['vitality_index'] > 85 and row['impact_weight'] > 2.0:
            return "STRATEGIC ASSET: High-performing anchor sector."
        else:
            return "STABLE: Standard maintenance of operations."

    nexus['strategic_action'] = nexus.apply(get_action, axis=1)
    
    # 6. PERSISTENCE
    os.makedirs('data', exist_ok=True)
    nexus.to_csv('data/calgary_strategy_kpis.csv', index=False)
    print("Nexus Build Successful: data/calgary_strategy_kpis.csv ready.")

if __name__ == "__main__":
    run_pipeline()
