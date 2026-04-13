import pandas as pd
import requests
import os
import sys

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
    
    # Expanded Select list to include all your requested fields
    params = {
        "$select": "comdistnm, jobstatusdesc, licencetypes, tradename, address, first_iss_dt",
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
        print(f"Extraction Successful: Found {len(df)} records.")
    except Exception as e:
        print(f"CRITICAL EXTRACTION ERROR: {e}")
        sys.exit(1)

    # Clean headers and find columns
    df.columns = [c.replace('_', '').lower() for c in df.columns]
    
    # Process Atomic Records
    df['industry_sector'] = df['licencetypes'].fillna('UNKNOWN').apply(get_industry_sector)
    
    # Add a Binary Metric for Tableau (1 for Active, 0 for Friction/Pending)
    df['is_active_binary'] = df['jobstatusdesc'].str.contains('Licensed', case=False).astype(int)

    # Save the FULL atomic data so Tableau can filter by Trade Name or Address
    output_dir = 'data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    file_path = os.path.join(output_dir, 'calgary_strategy_kpis.csv')
    df.to_csv(file_path, index=False)
    
    if os.path.exists(file_path):
        print(f"Nexus Build Complete: {file_path}")
    else:
        print("ERROR: File creation failed.")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
