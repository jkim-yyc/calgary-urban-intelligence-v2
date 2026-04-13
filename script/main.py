import pandas as pd
import requests
import os
import sys

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
    # Expanded list of potential dataset IDs (Legacy + Current + Related)
    endpoints = ["g5zp-p86c", "864q-7v6g", "6h66-y7v6", "kxpt-p8m8"]
    df = None

    for eid in endpoints:
        try:
            # Try JSON first
            url = f"https://data.calgary.ca/resource/{eid}.json?$limit=100000"
            print(f"Checking endpoint: {url}")
            r = requests.get(url, timeout=30)
            if r.status_code == 200 and len(r.json()) > 0:
                df = pd.DataFrame(r.json())
                break
            
            # Try CSV fallback for the same ID
            csv_url = f"https://data.calgary.ca/resource/{eid}.csv?$limit=100000"
            r_csv = requests.get(csv_url, timeout=30)
            if r_csv.status_code == 200:
                from io import StringIO
                df = pd.read_csv(StringIO(r_csv.text))
                break
        except Exception as e:
            print(f"Error on {eid}: {e}")
            continue

    if df is None or df.empty:
        print("CRITICAL: All extraction methods failed. The API may be down or the IDs have changed.")
        sys.exit(1)

    # Normalize column names to lowercase with no underscores
    df.columns = [c.replace('_', '').lower() for c in df.columns]
    
    # Advanced Column Finder (Looks for keywords in column names)
    comm_col = next((c for c in df.columns if any(k in c for k in ['comm', 'dist', 'ward'])), None)
    status_col = next((c for c in df.columns if 'status' in c), None)
    type_col = next((c for c in df.columns if any(k in c for k in ['type', 'tid', 'class'])), None)

    if not all([comm_col, status_col, type_col]):
        print(f"Mapping Failure. Found: Comm={comm_col}, Status={status_col}, Type={type_col}")
        sys.exit(1)

    # Convert columns to string to avoid attribute errors during mapping
    df[type_col] = df[type_col].astype(str)
    df[status_col] = df[status_col].astype(str)

    # Atomic record mapping
    df['industry_sector'] = df[type_col].apply(get_industry_sector)

    # Aggregation to Community + Sector
    nexus = df.groupby([comm_col, 'industry_sector']).agg(
        active_licenses=(status_col, lambda x: (x.str.contains('Issued', case=False, na=False)).sum()),
        churn_events=(status_col, lambda x: (x.str.contains('Cancelled|Expired', case=False, na=False)).sum()),
        total_volume=(status_col, 'count')
    ).reset_index()

    # Calculation Layer
    nexus['churn_rate'] = (nexus['churn_events'] / nexus['total_volume']).fillna(0)
    nexus['vitality_index'] = ((nexus['active_licenses'] / nexus['total_volume']) * 100).round(2)
    
    # Impact Weighting (Community scale vs Sector Average)
    avg_vol = nexus['total_volume'].mean()
    nexus['impact_weight'] = (nexus['total_volume'] / avg_vol).round(2)
    
    # Strategic Recommendation Logic
    def get_action(row):
        if row['churn_rate'] > 0.35 and row['impact_weight'] > 1.5:
            return "URGENT: High-impact sector failure. Immediate policy review recommended."
        elif row['churn_rate'] > 0.35:
            return "MONITOR: Elevated volatility in niche cluster."
        elif row['vitality_index'] > 85 and row['impact_weight'] > 2.0:
            return "STRENGTH: Significant economic anchor. Optimize for growth."
        else:
            return "STABLE: Performant sector. No immediate action."

    nexus['strategic_action'] = nexus.apply(get_action, axis=1)
    
    # Output
    os.makedirs('data', exist_ok=True)
    nexus.to_csv('data/calgary_strategy_kpis.csv', index=False)
    print("Success: Pipeline generated strategic output.")

if __name__ == "__main__":
    run_pipeline()
