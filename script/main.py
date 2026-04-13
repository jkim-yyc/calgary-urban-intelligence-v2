import pandas as pd
import requests
import os

API_URL = "https://data.calgary.ca/resource/6h66-y7v6.json"

def get_sector(lt):
    lt = str(lt).upper()
    mapping = {
        'Food & Beverage': ['FOOD', 'RESTAURANT', 'CAFE', 'BAKERY'],
        'Retail': ['RETAIL', 'STORE', 'SHOP'],
        'Health': ['MEDICAL', 'HEALTH', 'CLINIC', 'DENTAL'],
        'Personal Services': ['SALON', 'SPA', 'BARBER', 'BEAUTY'],
        'Construction': ['CONTRACTOR', 'CONSTRUCTION', 'PLUMBING'],
        'Automotive': ['AUTO', 'VEHICLE', 'MECHANIC'],
        'Professional': ['CONSULTING', 'LEGAL', 'ACCOUNTANT'],
        'Cannabis & Liquor': ['CANNABIS', 'LIQUOR', 'BREWERY']
    }
    for sector, keywords in mapping.items():
        if any(kw in lt for kw in keywords): return sector
    return 'Other'

def run_pipeline():
    try:
        # Step 1: Get Data
        r = requests.get(API_URL, params={"$limit": 50000})
        df = pd.DataFrame(r.json())
        
        # Step 2: Fix Column Names (removes underscores and lowercase everything)
        df.columns = [c.replace('_', '').lower() for c in df.columns]
        
        # Step 3: Find the Community column even if the name changes
        geo_col = next((c for c in df.columns if 'comm' in c), None)
        if not geo_col: return

        # Step 4: Map Sectors & Aggregate
        df['industry_sector'] = df['licencetypes'].apply(get_sector)
        nexus = df.groupby([geo_col, 'industry_sector']).agg(
            active=('licencestatus', lambda x: (x == 'Issued').sum()),
            total=('licencestatus', 'count')
        ).reset_index()

        # Step 5: Save File (The most important part)
        os.makedirs('data', exist_ok=True)
        nexus.to_csv('data/calgary_strategy_kpis.csv', index=False)
        print("Success: File created.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_pipeline()
