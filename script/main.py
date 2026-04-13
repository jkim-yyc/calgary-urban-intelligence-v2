import pandas as pd
import requests
import os

API_URL = "https://data.calgary.ca/resource/6h66-y7v6.json"

def get_industry_sector(lt):
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

def run_nexus_pipeline():
    print("--- Nexus Pipeline: Initializing ---")
    try:
        # Step 1: Data Acquisition
        response = requests.get(API_URL, params={"$limit": 50000})
        df = pd.DataFrame(response.json())
        
        if df.empty:
            print("ERROR: API returned an empty dataset.")
            return

        # Step 2: Clean and Normalize Columns
        df.columns = [c.replace('_', '').lower() for c in df.columns]
        
        # Determine the geographic column (Calgary API uses various names)
        geo_cols = ['comdistnm', 'communityname', 'community_name']
        found_geo = next((c for c in geo_cols if c in df.columns), None)
        
        if not found_geo:
            print(f"ERROR: No community column found. Available: {df.columns.tolist()}")
            return
        
        df.rename(columns={found_geo: 'community_district'}, inplace=True)

        # Step 3: Apply Industry Mapping
        # Handle missing 'licencetypes' by filling with 'UNKNOWN'
        df['licencetypes'] = df.get('licencetypes', 'UNKNOWN').fillna('UNKNOWN')
        df['industry_sector'] = df['licencetypes'].apply(get_industry_sector)

        # Step 4: Strategic Aggregation
        nexus_data = df.groupby(['community_district', 'industry_sector']).agg(
            active_licenses=('licencestatus', lambda x: (x == 'Issued').sum()),
            churn_events=('licencestatus', lambda x: x.isin(['Cancelled', 'Expired']).sum()),
            total_volume=('licencestatus', 'count')
        ).reset_index()

        nexus_data['churn_rate'] = (nexus_data['churn_events'] / nexus_data['total_volume']).fillna(0)
        
        # Step 5: Save Output
        # Use a path relative to the repo root
        output_dir = 'data'
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, 'calgary_strategy_kpis.csv')
        
        nexus_data.to_csv(file_path, index=False)
        
        if os.path.exists(file_path):
            print(f"SUCCESS: Intelligence Nexus file saved at {file_path}")
        else:
            print("ERROR: File was not successfully written to disk.")

    except Exception as e:
        print(f"CRITICAL SYSTEM ERROR: {str(e)}")

if __name__ == "__main__":
    run_nexus_pipeline()
