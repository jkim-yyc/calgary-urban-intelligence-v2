import pandas as pd
import requests
import os

def get_industry_sector(lt):
    """Categorizes raw licence types into Strategic Sectors."""
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
    url = "https://data.calgary.ca/resource/6h66-y7v6.json"
    try:
        print("Step 1: Fetching Calgary Open Data...")
        r = requests.get(url, params={"$limit": 50000})
        df = pd.DataFrame(r.json())
        
        # Normalize columns (lowercase and remove underscores)
        df.columns = [c.replace('_', '').lower() for c in df.columns]
        
        # Dynamic Column Finding: Find the community column regardless of API changes
        comm_col = next((c for c in df.columns if 'comm' in c), None)
        status_col = 'licencestatus' if 'licencestatus' in df.columns else df.columns[2]
        type_col = 'licencetypes' if 'licencetypes' in df.columns else None

        if not comm_col or not type_col:
            print("Error: Required columns not found.")
            return

        print(f"Step 2: Analyzing {len(df)} rows for Community: {comm_col}")
        
        # Apply Mapping
        df['industry_sector'] = df[type_col].apply(get_industry_sector)

        # Step 3: Strategic Aggregation
        nexus = df.groupby([comm_col, 'industry_sector']).agg(
            active_licenses=(status_col, lambda x: (x == 'Issued').sum()),
            churn_events=(status_col, lambda x: x.isin(['Cancelled', 'Expired']).sum()),
            total_volume=(status_col, 'count')
        ).reset_index()

        nexus['churn_rate'] = (nexus['churn_events'] / nexus_data['total_volume']).fillna(0)
        
        # Step 4: Force Save to /data
        os.makedirs('data', exist_ok=True)
        nexus.to_csv('data/calgary_strategy_kpis.csv', index=False)
        print("SUCCESS: Strategic Intelligence file created.")

    except Exception as e:
        print(f"PIPELINE ERROR: {e}")

if __name__ == "__main__":
    run_pipeline()
