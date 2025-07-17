import pandas as pd

def fix_additional_urls():
    """Fix additional problematic URLs"""
    
    # Load the CSV
    df = pd.read_csv('companies_final_ready.csv')
    print(f"✅ Loaded {len(df)} companies")
    
    # Additional URL fixes
    additional_fixes = {
        'TeamSnap': 'https://jobs.lever.co/teamsnap',
        'LenDesk': 'https://lendesk.bamboohr.com/careers',
        'Datadog': 'https://careers.datadoghq.com/all-jobs/?s=',
        'Power Takeoff': 'https://ats.rippling.com/power-takeoff-careers/jobs',
        'Seequent': 'https://seequent.csod.com/ux/ats/careersite/1/home?c=seequent',
        'Sardine': 'https://www.sardine.ai/careers#openings',
        'Sunbit': 'https://sunbit.com/careers-il/#job-feed',
        'TTEC': 'https://ttec.taleo.net/careersection/2/jobsearch.ftl'
    }
    
    print(f"\n🔧 Fixing {len(additional_fixes)} additional URLs:")
    
    # Apply URL fixes
    for company, new_url in additional_fixes.items():
        mask = df['Company'] == company
        if mask.any():
            df.loc[mask, 'Careers Site URL'] = new_url
            print(f"  ✅ Fixed URL for {company}")
        else:
            print(f"  ❌ Company not found: {company}")
    
    # Save the updated CSV
    df.to_csv('companies_final_ready.csv', index=False)
    print(f"\n✅ Updated CSV saved")

if __name__ == "__main__":
    fix_additional_urls()
