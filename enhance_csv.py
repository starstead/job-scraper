import pandas as pd
import json

def enhance_csv_for_multiplatform():
    """Add company size and multi-platform URLs to existing CSV"""
    
    # Load current CSV
    df = pd.read_csv('companies.csv')
    print(f"Loaded {len(df)} companies")
    
    # Add new columns for multi-platform approach
    new_columns = {
        'Company_Size': '',
        'Primary_Source': '',
        'Indeed_URL': '',
        'AngelList_URL': '', 
        'Glassdoor_URL': '',
        'Backup_Strategy': ''
    }
    
    for col, default_val in new_columns.items():
        if col not in df.columns:
            df[col] = default_val
    
    # Company size classifications
    large_companies = {
        'palantir': 'Large',
        'snowflake': 'Large', 
        'datadog': 'Large',
        'data dog': 'Large',
        'twilio': 'Large',
        'alteryx': 'Large',
        'fastly': 'Large',
        'huntington bank': 'Large',
        'thomson reuters': 'Large',
        'siemens': 'Large',
        'l3harris': 'Large',
        'general dynamics': 'Large',
        'uipath': 'Large',
        'tyler tech': 'Large',
        'genesys': 'Large',
        'proofpoint': 'Large',
        'experian': 'Large',
        'trimble': 'Large',
        'meltwater': 'Large'
    }
    
    small_companies = {
        'burstiq': 'Small',
        'josh.ai': 'Small', 
        'absio': 'Small',
        'aquaoso': 'Small',
        'argis solutions': 'Small',
        'brightwave': 'Small',
        'comtrac': 'Small',
        'darkowl': 'Small',
        'emite': 'Small',
        'flytedesk': 'Small',
        'gridics': 'Small',
        'homebot': 'Small',
        'kaseware': 'Small',
        'neat capital': 'Small',
        'precog': 'Small',
        'phase change software': 'Small',
        'serve robotics': 'Small',
        'smartwyre': 'Small',
        'ombud': 'Small',
        'redeam': 'Small',
        'switch automation': 'Small',
        'the receptionist': 'Small',
        'truid': 'Small',
        'hosify': 'Small'
    }
    
    # Process each company
    for idx, row in df.iterrows():
        company_name = str(row['Company']).lower().strip()
        industry = row.get('Industry', '')
        careers_url = row.get('Careers Site URL', '')
        city = row.get('City', 'Denver')
        
        # Determine company size
        if company_name in large_companies:
            company_size = 'Large'
        elif company_name in small_companies:
            company_size = 'Small'
        else:
            # Industry-based heuristics
            if industry in ['Analytics & AI', 'Cybersecurity', 'Real Estate Technology']:
                company_size = 'Small'
            elif industry in ['Public Safety', 'Enterprise Software', 'Communications']:
                company_size = 'Medium'
            else:
                company_size = 'Medium'  # Default
        
        df.at[idx, 'Company_Size'] = company_size
        
        # Determine primary source
        if pd.isna(careers_url) or not careers_url or careers_url == 'nan':
            df.at[idx, 'Primary_Source'] = 'job_boards'
        else:
            df.at[idx, 'Primary_Source'] = 'careers_page'
        
        # Create multi-platform URLs
        company_display = row['Company']
        location = f"{city},CO" if city and city != 'nan' else "Denver,CO"
        
        # Indeed company search
        indeed_url = f'https://www.indeed.com/jobs?q=company:"{company_display}"&l={location}'
        df.at[idx, 'Indeed_URL'] = indeed_url
        
        # AngelList (for small companies)
        if company_size == 'Small':
            company_slug = company_display.lower().replace(' ', '-').replace('.', '').replace(',', '').replace('(', '').replace(')', '')
            angellist_url = f'https://angel.co/company/{company_slug}/jobs'
            df.at[idx, 'AngelList_URL'] = angellist_url
        
        # Glassdoor company search
        glassdoor_search = company_display.replace(' ', '%20').replace('&', '%26')
        glassdoor_url = f'https://www.glassdoor.com/Jobs/{company_display.replace(" ", "-")}-Jobs-E12345.htm'
        df.at[idx, 'Glassdoor_URL'] = glassdoor_url
        
        # Backup strategy based on company size
        if company_size == 'Large':
            df.at[idx, 'Backup_Strategy'] = 'careers_page_only'
        elif company_size == 'Medium':
            df.at[idx, 'Backup_Strategy'] = 'careers_page,indeed'
        else:  # Small
            df.at[idx, 'Backup_Strategy'] = 'indeed,angellist,glassdoor'
    
    # Generate summary
    print(f"\n=== ENHANCEMENT SUMMARY ===")
    print(f"Total companies: {len(df)}")
    print(f"\nCompany sizes:")
    size_counts = df['Company_Size'].value_counts()
    for size, count in size_counts.items():
        print(f"  {size}: {count} companies")
    
    print(f"\nPrimary sources:")
    source_counts = df['Primary_Source'].value_counts()
    for source, count in source_counts.items():
        print(f"  {source}: {count} companies")
    
    print(f"\nBackup strategies:")
    strategy_counts = df['Backup_Strategy'].value_counts()
    for strategy, count in strategy_counts.items():
        print(f"  {strategy}: {count} companies")
    
    # Save enhanced CSV
    df.to_csv('companies_enhanced.csv', index=False)
    print(f"\nSaved enhanced CSV: companies_enhanced.csv")
    
    # Create action plan
    create_multiplatform_action_plan(df)
    
    return df

def create_multiplatform_action_plan(df):
    """Create action plan for multi-platform scraping"""
    
    print(f"\n=== MULTI-PLATFORM ACTION PLAN ===")
    
    # Companies by strategy
    careers_only = df[df['Backup_Strategy'] == 'careers_page_only']
    careers_indeed = df[df['Backup_Strategy'] == 'careers_page,indeed']
    full_multiplatform = df[df['Backup_Strategy'] == 'indeed,angellist,glassdoor']
    
    print(f"\n1. Large Companies (Careers Page Focus): {len(careers_only)}")
    print("   Strategy: Primary careers page only")
    print("   Expected: 5-15 jobs per company")
    
    print(f"\n2. Medium Companies (Hybrid Approach): {len(careers_indeed)}")
    print("   Strategy: Careers page + Indeed backup") 
    print("   Expected: 3-10 jobs per company")
    
    print(f"\n3. Small Companies (Multi-Platform): {len(full_multiplatform)}")
    print("   Strategy: Indeed + AngelList + Glassdoor")
    print("   Expected: 1-8 jobs per company")
    
    # Broken careers pages that need job board focus
    job_board_primary = df[df['Primary_Source'] == 'job_boards']
    print(f"\n4. Job Board Primary: {len(job_board_primary)} companies")
    print("   These will rely entirely on Indeed/AngelList/Glassdoor")
    
    # Expected total jobs
    large_jobs = len(careers_only) * 8  # Average
    medium_jobs = len(careers_indeed) * 6  # Average
    small_jobs = len(full_multiplatform) * 4  # Average
    job_board_jobs = len(job_board_primary) * 3  # Average
    
    total_expected = large_jobs + medium_jobs + small_jobs + job_board_jobs
    
    print(f"\n=== EXPECTED RESULTS ===")
    print(f"Current: ~54 jobs found")
    print(f"Multi-platform expected: ~{total_expected} jobs")
    print(f"Improvement: {total_expected/54:.1f}x increase")
    
    print(f"\n=== NEXT STEPS ===")
    print("1. Replace companies.csv with companies_enhanced.csv")
    print("2. Update job_scraper.py to use multi-platform approach")
    print("3. Test with a few companies first")
    print("4. Run full scan and expect 200-400+ jobs")

def fix_config_json():
    """Fix the config.json syntax error"""
    
    config = {
        "keywords": [
            "product manager",
            "senior product manager", 
            "associate product manager",
            "product marketing manager",
            "product owner",
            "product analyst",
            "software engineer",
            "senior software engineer",
            "frontend engineer",
            "backend engineer",
            "fullstack engineer",
            "data analyst",
            "data scientist", 
            "business analyst",
            "marketing manager",
            "growth manager",
            "remote",
            "hybrid",
            "python",
            "javascript",
            "react",
            "node.js",
            "sql",
            "aws",
            "machine learning",
            "artificial intelligence"
        ],
        "settings": {
            "scan_frequency": "daily",
            "use_multiplatform": True,
            "indeed_enabled": True,
            "angellist_enabled": True,
            "glassdoor_enabled": True
        }
    }
    
    with open('config_fixed.json', 'w') as f:
        json.dump(config, f, indent=2)
    
    print("Fixed config.json saved as config_fixed.json")

if __name__ == "__main__":
    enhance_csv_for_multiplatform()
    fix_config_json()
