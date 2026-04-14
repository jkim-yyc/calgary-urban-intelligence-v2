import pandas as pd
import numpy as np

def calculate_nexus_metrics(df):
    """
    Transforms raw Calgary business data into CSO-level Strategic Intelligence.
    """
    # 1. Normalize Footprint (Impact Weight)
    df['strategic_footprint'] = df['impact_weight'] / df['impact_weight'].max()

    # 2. Normalize Resilience (Vitality Index)
    df['systemic_resilience'] = df['vitality_index'] / df['vitality_index'].max()

    # 3. Expansion Momentum (Growth Count)
    # Using a 0-1 scale for the integrated score
    df['expansion_momentum'] = df['growth_count'] / df['growth_count'].max()

    # 4. Innovation Acceleration (Sector Velocity)
    # Logic: How much faster is this node firing compared to the city average?
    city_avg_growth = df['growth_count'].mean()
    df['innovation_acceleration'] = (df['growth_count'] / city_avg_growth) * df['systemic_resilience']
    df['innovation_acceleration'] = df['innovation_acceleration'].clip(upper=2.0)

    # 5. Integrated Health Score (Weighted)
    # Footprint (20%), Resilience (30%), Acceleration (30%), Momentum (20%)
    df['health_score'] = (
        (df['strategic_footprint'] * 0.2) + 
        (df['systemic_resilience'] * 0.3) + 
        (df['innovation_acceleration'] * 0.3) + 
        (df['expansion_momentum'] * 0.2)
    )

    # 6. Strategic Action Logic
    def get_action(row):
        if row['systemic_resilience'] < 0.3 and row['strategic_footprint'] > 0.6:
            return "URGENT INTERVENTION"
        elif row['expansion_momentum'] > 0.8 and row['systemic_resilience'] < 0.4:
            return "STABILIZE EXPANSION"
        elif row['innovation_acceleration'] > 1.2 and row['systemic_resilience'] > 0.6:
            return "STABLE OPERATIONS"
        else:
            return "MONITOR FRICTION"

    df['strategic_action'] = df.apply(get_action, axis=1)
    
    return df

if __name__ == "__main__":
    # Load your Calgary data
    raw_data = pd.read_csv('calgary_business_data.csv')
    
    # Process Neural Metrics
    processed_df = calculate_nexus_metrics(raw_data)
    
    # Export for Tableau
    processed_df.to_csv('nexus_intelligence_feed.csv', index=False)
    print("Nexus Intelligence Feed Updated: [SUCCESS]")
