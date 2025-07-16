import pandas as pd
import json

def clean_companies_csv():
    """Clean up companies CSV by removing unwanted companies and fixing URLs"""
    
    # Load the CSV
    try:
        df = pd.read_csv('companies_final_ready.csv')
        print(f"✅ Loaded {len(df)} companies")
    except FileNotFoundError:
        print("❌ companies_final_ready.csv not found!")
        return
    
    # Companies to remove
    companies_to_remove = [
        'Neat Capital',
        'Open Text', 
        'Orderly',
        'Pathify'
    ]
    
    print(f"\n🗑️ Removing {len(companies_to_remove)} companies:")
    for company in companies_to_remove:
        print(f"  - {company}")
    
    # Remove unwanted companies
    initial_count = len(df)
    df = df[~df['Company'].isin(companies_to_remove)]
    removed_count = initial_count - len(df)
    print(f"✅ Removed {removed_count} companies")
    
    # URL fixes/updates
    url_fixes = {
        'CQG': 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=d941071c-bf35-4bb0-a7bb-4e0480b279fe&ccId=19000101_000001&type=JS&lang=en_US',
        'D3 Security': 'https://d3security.com/company/careers/',
        'Dark Owl': 'https://jobs.gusto.com/boards/who-is-darkowl-29ab8fbb-1a9f-4a05-8207-3ea0fadc29d5',
        'DataVisor': 'https://www.datavisor.com/about4/careers/#jobs',
        'FirstArriving': 'https://firstarriving.com/jobs/',
        'Five9': 'https://www.five9.com/about/careers/jobs#grnhse_app',
        'FluentStream': 'https://fluentstream.applytojob.com/apply/Wr5ru22GGZ/Future-Opportunities?source=Our%20Career%20Page%20Widget',
        'ForceMetrics': 'https://apply.workable.com/forcemetrics/',
        'Frontsteps': 'https://jobs.lever.co/frontsteps',
        'GigSmart': 'https://jobs.gigsmart.com/',
        'Gusto': 'https://gusto.com/about/careers/join-the-team',
        'Healthgrades': 'https://www.rvohealth.com/careers#open-positions',
        'Huntington National Bank': 'https://huntington-careers.com/search/searchjobs?geolocationstring=39.87199020385742%2C-104.92466735839844_Denver%2C+CO&radius=25',
        'IQware': 'https://iqwareinc.com/about/careers/',
        'Josh.ai': 'https://www.josh.ai/jobs/',
        'Kantox': 'https://www.kantox.com/careers#job-openings',
        'LexisNexis': 'https://www.lexisnexis.com/systems/careers/job-search.html',
        'Team Liquid': 'https://careers.teamliquid.com/jobs',
        'Macrium Software': 'https://www.macrium.com/careers-and-culture',
        'Mark43': 'https://mark43.com/company/careers/#greenhouse-jobs',
        'Maxwell': 'https://ats.rippling.com/maxwell-careers/jobs',
        'Mersive': 'https://www.mersive.com/career/#open-positions',
        'Mitel': 'https://mitel.wd3.myworkdayjobs.com/mitelcareers',
        'mPulse': 'https://mpulseportal.rec.pro.ukg.net/MPU1000PULSE/JobBoard/dc48557c-996a-4e30-a53d-fd3dfb186808/?q=&o=postedDateDesc&w=&wc=&we=&wpst=',
        'Navigator Business Solutions': 'https://www.nbs-us.com/careers-2',
        'NICE': 'https://www.nice.com/careers/apply',
        'PhaseChange Software': 'https://phasechangesoftwarellc.applytojob.com/apply',
        'Planet': 'https://www.planet.com/company/careers/?depId=228529',
        'Precog': 'https://precog.com/careers/'
    }
    
    print(f"\n🔧 Fixing URLs for {len(url_fixes)} companies:")
    
    # Apply URL fixes
    for company, new_url in url_fixes.items():
        mask = df['Company'] == company
        if mask.any():
            df.loc[mask, 'Careers Site URL'] = new_url
            print(f"  ✅ Fixed URL for {company}")
        else:
            print(f"  ❌ Company not found: {company}")
    
    # Save the cleaned CSV
    df.to_csv('companies_final_ready.csv', index=False)
    print(f"\n✅ Saved cleaned CSV with {len(df)} companies")
    
    # Create a summary
    print(f"\n📊 CLEANUP SUMMARY:")
    print(f"  - Original companies: {initial_count}")
    print(f"  - Removed companies: {removed_count}")
    print(f"  - Final companies: {len(df)}")
    print(f"  - URLs fixed: {len(url_fixes)}")
    
    return df

if __name__ == "__main__":
    clean_companies_csv()
