import pandas as pd
import requests
import os

# Calgary Open Data API - Business Licenses
API_URL = "https://data.calgary.ca/resource/6h66-y7v6.json"

def run_nexus_pipeline():
    # 1. Fetch data
    try:
        # Increase limit to capture a broader strategic sample
        response = requests.get(API_URL, params={"$limit": 50000})
        df = pd.DataFrame(response.json())
    except Exception as e:
        print(f"Connection Error: {e}")
        return

    if df.empty:
        print("Error: No data returned from API.")
        return

    # 2. Normalize Column Names (The "CSO Resilience" Step)
    # This removes underscores and forces lowercase to prevent KeyErrors
    df.columns = [c.replace('_', '').lower() for c in df.columns]

    # 3. Strategic Aggregation
    # Now we can safely use 'communityname' and 'licencestatus'
    try:
        nexus_data = df.groupby('communityname').agg(
            active_licenses=('licencestatus', lambda x: (x == 'Issued').sum()),
            churn_events=('licencestatus', lambda x: x.isin(['Cancelled', 'Expired']).sum()),
            total_volume=('licencestatus', 'count')
        ).reset_index()

        # KPI: Churn Rate
        nexus_data['churn_rate'] = (nexus_data['churn_events'] / nexus_data['total_volume']).fillna(0)
        
        # KPI: Retail Density
        # Using 'licencetid' normalized to 'licencetid'
        retail = df[df['licencetid'] == 'RETAIL'].groupby('communityname').size().reset_index(name='retail_density')
        final_output = pd.merge(nexus_data, retail, on='communityname', how='left').fillna(0)

        # 4. Save Output
        os.makedirs('data', exist_ok=True)
        final_output.to_csv('data/calgary_strategy_kpis.csv', index=False)
        print("Success: Strategic KPIs generated.")
        
    except KeyError as e:
        print(f"Column Name Error: {e}. Current columns are: {list(df.columns)}")

if __name__ == "__main__":
    run_nexus_pipeline()
