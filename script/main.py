def get_industry_sector(lt):
    """Calgary-specific keyword mapping to fix 'Diversified/Other' issues."""
    lt = str(lt).upper()
    
    mapping = {
        # TECH & INTELLIGENCE (Often categorized as 'Professional Services' or 'Office')
        'Software & SaaS': ['SOFTWARE', 'SAAS', 'COMPUTER', 'TECHNOLOGY'],
        'Data & AI Services': ['DATA', 'ANALYTICS', 'CONSULTING'],
        
        # FOOD & BEVERAGE (Calgary uses 'FOOD SERVICE' and 'DRINKING ESTABLISHMENT')
        'Fine Dining': ['FOOD SERVICE', 'RESTAURANT', 'DINING'],
        'Nightlife & Bars': ['DRINKING ESTABLISHMENT', 'NIGHTCLUB', 'PUB', 'BAR'],
        'Cafes & Bakeries': ['BAKERY', 'COFFEE', 'CAFE'],
        
        # RETAIL (Calgary uses 'RETAIL STORE' frequently)
        'General Merchandise': ['RETAIL STORE', 'MARKET', 'SHOP'],
        'Automotive Sales': ['MOTOR VEHICLE', 'DEALER', 'AUTO'],
        'Liquor Retail': ['LIQUOR', 'WINE', 'BEER'],
        'Cannabis Retail': ['CANNABIS', 'MARIJUANA'],
        
        # TRADES (Calgary uses 'CONTRACTOR')
        'Civil Construction': ['CONSTRUCTION', 'INFRASTRUCTURE'],
        'Residential Trades': ['CONTRACTOR', 'PLUMBING', 'ELECTRICAL', 'HVAC'],
        'Oil & Gas Services': ['ENERGY', 'OIL', 'GAS', 'PETROLEUM'],
        
        # SERVICES
        'Personal Grooming': ['PERSONAL SERVICE', 'HAIR', 'SALON', 'BARBER'],
        'Medical Services': ['HEALTH', 'MEDICAL', 'CLINIC', 'DENTAL'],
        'Legal & Financial': ['LEGAL', 'ACCOUNTING', 'FINANCIAL', 'BANK'],
    }
    
    for sector, keywords in mapping.items():
        if any(kw in lt for kw in keywords):
            return sector
            
    # If no keywords match, return the raw value temporarily to help us debug
    return lt if lt != "UNKNOWN" else 'Diversified/Other'
