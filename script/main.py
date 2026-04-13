import pandas as pd
import requests
import os

# Calgary Open Data API - Business Licenses
API_URL = "https://data.calgary.ca/resource/6h66-y7v6.json"

def run_nexus_pipeline():
    # 1. Fetch data (50k records for deep analysis)
    try:
        response = requests.get(API_URL, params={"$limit": 50000})
        df = pd.DataFrame(response.json())
    except Exception as e:
        print(f"Connection Error: {e}")
        return

    # 2. Strategic Aggregation (KPI Logic)
    # Group by community to turn license-level data into strategy-level insights
    nexus_data = df.groupby('communityname').agg(
        active_licenses=('licencestatus', lambda x: (x == 'Issued').sum()),
        churn_events=('licencestatus', lambda x: x.isin(['Cancelled', 'Expired']).sum()),
        total_volume=('licencestatus', 'count')
    ).reset_index()

    # KPI: Churn Rate
    nexus_data['churn_rate'] = (nexus_data['churn_events'] / nexus_data['total_volume']).fillna(0)
    
    # KPI: Retail Density (Market Gap Indicator)
    retail = df[df['licencetid'] == 'RETAIL'].groupby('communityname').size().reset_index(name='retail_density')
    final_output = pd.merge(nexus_data, retail, on='communityname', how='left').fillna(0)

    # 3. Save Output - Ensure the directory exists and the string is closed
    os.makedirs('data', exist_ok=True)
    final_output.to_csv('data/calgary_strategy_kpis.csv', index=False)
    print("Success: CSV updated in /data folder.")

if __name__ == "__main__":
    run_nexus_pipeline()
