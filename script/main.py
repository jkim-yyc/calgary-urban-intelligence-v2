import pandas as pd
import requests
import os
import sys
from io import StringIO

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
    # Try the most likely active dataset IDs
    endpoints = ["g5zp-p86c", "864q-7v6g", "kxpt-p8m8"]
    df = None

    for eid in endpoints:
        try:
            # Try JSON format
            url = f"https://data.calgary.ca/resource/{eid}.json?$limit=100000"
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if data:
                    df = pd.DataFrame(data)
                    print(f"Success: Data pulled from {eid} (JSON)")
                    break
            
            # Fallback to CSV format for the same ID
            csv_url = f"https://data.calgary.ca/resource/{eid}.csv?$limit=100000"
            r_csv = requests.get(csv_url, timeout=30)
            if r_csv.status_code == 200:
                df = pd.read_csv(StringIO(r_csv.text))
                print(f"Success: Data pulled from {eid} (CSV)")
                break
        except Exception as e:
            print(f"Failed {eid}: {e}")
            continue

    if df is None or df.empty:
        print("CRITICAL: All extraction methods failed.")
        sys.exit(1)

    # Clean headers and find columns
    df.columns = [c.replace('_', '').lower() for c in df.columns]
    comm_col = next((c for c in df.columns if any(k in c for k in ['comm', 'dist', 'ward'])), None)
    status_col = next((c for c in df.columns if 'status' in c), None)
    type_col = next((c for c in df.columns if any(k in c for k in ['type', 'tid', 'class'])), None)

    # Core Logic
    df['industry_sector'] = df[type_col].astype(str).apply(get_industry_sector)

    # Aggregation
    nexus = df.groupby([comm_col, 'industry_sector']).agg(
        active_licenses=(status_col, lambda x: (x.astype(str).str.contains('Issued', case=False)).sum()),
        churn_events=(status_col, lambda x: (x.astype(str).str.contains('Cancelled|Expired', case=False)).sum()),
        total_volume=(status_col, 'count')
    ).reset_index()

    # Strategic Metrics
    nexus['churn_rate'] = (nexus['churn_events'] / nexus['total_volume']).fillna(0)
    nexus['vitality_index'] = ((nexus['active_licenses'] / nexus['total_volume']) * 100).round(2)
    avg_vol = nexus['total_volume'].mean()
    nexus['impact_weight'] = (nexus['total_volume'] / avg_vol).round(2)

    def get_action(row):
        if row['churn_rate'] > 0.35 and row['impact_weight'] > 1.5:
            return "URGENT: High-impact sector failure. Priority 1."
        elif row['churn_rate'] > 0.35:
            return "MONITOR: Elevated volatility in local hub."
        elif row['vitality_index'] > 85 and row['impact_weight'] > 2.0:
            return "STRENGTH: Significant economic anchor."
        else:
            return "STABLE: Standard maintenance."

    nexus['strategic_action'] = nexus.apply(get_action, axis=1)

    os.makedirs('data', exist_ok=True)
    nexus.to_csv('data/calgary_strategy_kpis.csv', index=False)
    print("Pipeline Complete.")

if __name__ == "__main__":
    run_pipeline()
