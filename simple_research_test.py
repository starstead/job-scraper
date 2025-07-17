#!/usr/bin/env python3
"""
Quick test script to validate company research concepts
"""

import pandas as pd
from datetime import datetime

def create_targeted_company_list():
    """Create a targeted list of companies based on Anne and Alessandro's profiles"""
    
    # Anne's targeted companies (Arts/Cultural/Event Tech)
    anne_companies = [
        {
            'Company': 'PatronManager',
            'Industry': 'Arts Technology',
            'Careers Site URL': 'https://www.patronmanager.com/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Medium',
            'City': 'New York',
            'Why_Relevant_Anne': 'CRM for arts organizations, similar to Tessitura',
            'Focus': 'anne'
        },
        {
            'Company': 'Blackbaud',
            'Industry': 'Nonprofit Technology', 
            'Careers Site URL': 'https://www.blackbaud.com/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Large',
            'City': 'Charleston',
            'Why_Relevant_Anne': 'Nonprofit software, serves arts organizations',
            'Focus': 'anne'
        },
        {
            'Company': 'Cvent',
            'Industry': 'Event Technology',
            'Careers Site URL': 'https://www.cvent.com/careers',
            'Primary_Source': 'careers_page', 
            'Company_Size': 'Large',
            'City': 'Tysons',
            'Why_Relevant_Anne': 'Event management platform, enterprise events',
            'Focus': 'anne'
        },
        {
            'Company': 'EMS Software',
            'Industry': 'Higher Education Technology',
            'Careers Site URL': 'https://www.dea.com/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Medium', 
            'City': 'Collegeville',
            'Why_Relevant_Anne': 'University event/space management, leverages EdTech background',
            'Focus': 'anne'
        },
        {
            'Company': 'Brown Paper Tickets',
            'Industry': 'Event Technology',
            'Careers Site URL': 'https://www.brownpapertickets.com/jobs',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Small',
            'City': 'Seattle', 
            'Why_Relevant_Anne': 'Independent venue ticketing, similar to Spektrix market',
            'Focus': 'anne'
        }
    ]
    
    # Alessandro's targeted companies (Global/Fintech/Public Safety)
    alessandro_companies = [
        {
            'Company': 'Wise',
            'Industry': 'Global Fintech',
            'Careers Site URL': 'https://wise.com/us/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Large',
            'City': 'New York',
            'Why_Relevant_Alessandro': 'International money transfer, European roots, multilingual',
            'Focus': 'alessandro'
        },
        {
            'Company': 'Remitly', 
            'Industry': 'Global Fintech',
            'Careers Site URL': 'https://www.remitly.com/us/en/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Large',
            'City': 'Seattle',
            'Why_Relevant_Alessandro': 'Global remittances, compliance expertise, similar to Western Union',
            'Focus': 'alessandro'
        },
        {
            'Company': 'Klarna',
            'Industry': 'Consumer Fintech', 
            'Careers Site URL': 'https://www.klarna.com/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Large',
            'City': 'New York',
            'Why_Relevant_Alessandro': 'Swedish company US expansion, European operations',
            'Focus': 'alessandro'
        },
        {
            'Company': 'Tyler Technologies',
            'Industry': 'Government Technology',
            'Careers Site URL': 'https://www.tylertech.com/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Large', 
            'City': 'Plano',
            'Why_Relevant_Alessandro': 'Public safety software, government clients',
            'Focus': 'alessandro'
        },
        {
            'Company': 'Mark43',
            'Industry': 'Public Safety Technology',
            'Careers Site URL': 'https://mark43.com/company/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Medium',
            'City': 'New York',
            'Why_Relevant_Alessandro': 'Police software, similar to Motorola CommandCentral',
            'Focus': 'alessandro'
        },
        {
            'Company': 'Checkout.com',
            'Industry': 'Payment Infrastructure',
            'Careers Site URL': 'https://www.checkout.com/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Large',
            'City': 'San Francisco', 
            'Why_Relevant_Alessandro': 'UK payment company, global compliance, European operations',
            'Focus': 'alessandro'
        }
    ]
    
    # Combined companies for both
    both_companies = [
        {
            'Company': 'Stripe',
            'Industry': 'Payment Infrastructure',
            'Careers Site URL': 'https://stripe.com/jobs',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Large',
            'City': 'San Francisco',
            'Why_Relevant_Anne': 'Strong product organization, data-driven culture',
            'Why_Relevant_Alessandro': 'Global payments, European operations, compliance',
            'Focus': 'both'
        },
        {
            'Company': 'Notion',
            'Industry': 'Productivity Software', 
            'Careers Site URL': 'https://www.notion.so/careers',
            'Primary_Source': 'careers_page',
            'Company_Size': 'Medium',
            'City': 'San Francisco',
            'Why_Relevant_Anne': 'Product-focused, user experience driven',
            'Why_Relevant_Alessandro': 'Global expansion, international markets',
            'Focus': 'both'
        }
    ]
    
    # Combine all companies
    all_companies = anne_companies + alessandro_companies + both_companies
    
    # Create DataFrame
    df = pd.DataFrame(all_companies)
    
    # Add research metadata
    df['Research_Date'] = datetime.now().strftime('%Y-%m-%d')
    df['Research_Source'] = 'Targeted Research - Profile Based'
    
    return df

def save_targeted_research():
    """Save the targeted company research"""
    
    print("🎯 Creating targeted company list based on your profiles...")
    
    # Create the company list
    companies_df = create_targeted_company_list()
    
    # Save detailed version
    detailed_file = 'research_results/targeted_companies_detailed.csv'
    companies_df.to_csv(detailed_file, index=False)
    
    # Create integration version (matches your scraper format)
    integration_columns = ['Company', 'Careers Site URL', 'Industry', 'Primary_Source', 'Company_Size', 'City']
    integration_df = companies_df[integration_columns].copy()
    
    integration_file = 'research_results/targeted_companies_ready_to_integrate.csv'
    integration_df.to_csv(integration_file, index=False)
    
    # Create summary by focus
    summary = {
        'total_companies': len(companies_df),
        'anne_focused': len(companies_df[companies_df['Focus'] == 'anne']),
        'alessandro_focused': len(companies_df[companies_df['Focus'] == 'alessandro']), 
        'both_focused': len(companies_df[companies_df['Focus'] == 'both']),
        'research_date': datetime.now().isoformat()
    }
    
    print(f"\n📊 TARGETED RESEARCH COMPLETE!")
    print(f"Total companies: {summary['total_companies']}")
    print(f"Anne-focused: {summary['anne_focused']}")
    print(f"Alessandro-focused: {summary['alessandro_focused']}")
    print(f"Both-focused: {summary['both_focused']}")
    
    print(f"\n🎭 Anne's Top Companies:")
    anne_companies = companies_df[companies_df['Focus'].isin(['anne', 'both'])]
    for _, company in anne_companies.iterrows():
        print(f"  • {company['Company']} ({company['Industry']})")
    
    print(f"\n🌍 Alessandro's Top Companies:")
    alessandro_companies = companies_df[companies_df['Focus'].isin(['alessandro', 'both'])]
    for _, company in alessandro_companies.iterrows():
        print(f"  • {company['Company']} ({company['Industry']})")
    
    print(f"\n📁 Files created:")
    print(f"  • Detailed: {detailed_file}")
    print(f"  • Integration ready: {integration_file}")
    
    return companies_df

if __name__ == "__main__":
    import os
    os.makedirs('research_results', exist_ok=True)
    
    companies = save_targeted_research()
