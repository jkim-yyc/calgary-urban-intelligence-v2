import pandas as pd
import requests
import os

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

def run_pipeline():
    url = "https://data.calgary.ca/resource/6h66-y7v6.json"
    r = requests.get(url, params={"$limit": 50000})
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    
    # Normalize headers
    df.columns = [c.replace('_', '').lower() for c in df.columns]
    
    # Robust Atomic Column Detection
    # Searches for 'comm' (Community), 'status' (License Status), and 'type' (License Type)
    comm_col = next((c for c in df.columns if 'comm' in c), 'comdistnm')
    status_col = next((c for c in df.columns if 'status' in c), 'licencestatus')
    type_col = next((c for c in df.columns if 'type' in c), 'licencetypes')

    # Atomic-to-Sector Mapping
    df['industry_sector'] = df[type_col].apply(get_industry_sector)

    # Aggregation
    nexus = df.groupby([comm_col, 'industry_sector']).agg(
        active_licenses=(status_col, lambda x: (x.astype(str).str.contains('Issued', case=False)).sum()),
        churn_events=(status_col, lambda x: (x.astype(str).str.contains('Cancelled|Expired', case=False)).sum()),
        total_volume=(status_col, 'count')
    ).reset_index()

    # Community KPI Logic
    nexus['churn_rate'] = (nexus['churn_events'] / nexus['total_volume']).fillna(0)
    nexus['vitality_index'] = ((nexus['active_licenses'] / nexus['total_volume']) * 100).round(2)
    
    # Impact Weighting
    avg_vol = nexus['total_volume'].mean()
    nexus['impact_weight'] = (nexus['total_volume'] / avg_vol).round(2)
    
    # Prescriptive Action Logic
    def get_action(row):
        if row['churn_rate'] > 0.35 and row['impact_weight'] > 1.5:
            return "URGENT INTERVENTION: High systemic risk in high-volume hub."
        elif row['churn_rate'] > 0.35:
            return "MONITOR: Localized churn detected in small cluster."
        elif row['vitality_index'] > 85 and row['impact_weight'] > 2.0:
            return "STRATEGIC ASSET: High-performing anchor sector."
        else:
            return "STABLE: Standard maintenance of operations."

    nexus['strategic_action'] = nexus.apply(get_action, axis=1)
    
    os.makedirs('data', exist_ok=True)
    nexus.to_csv('data/calgary_strategy_kpis.csv', index=False)
    print("Nexus Build Successful.")

if __name__ == "__main__":
    run_pipeline()
