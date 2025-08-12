#!/usr/bin/env python3
"""
Corrected company cleanup script based on manual verification
Fixes broken URLs and removes problematic companies
"""

import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def apply_corrections():
    """Apply the verified corrections to the companies dataset"""
    
    # Companies to remove completely (confirmed non-existent or unusable)
    companies_to_remove = [
        'Catalyst Healthcare',  # Needs to be removed
        'KariVis',  # Should be removed 
        'EngageMetrics',  # Remove
        'SuccessFlow',  # Remove
        'PinnacleAI',  # Remove
        'Kiva',  # Remove (from issue #5)
        'Quizlet',  # Remove
        'MeetingWave',  # Remove
        'CustomerAI',  # Remove
        'MegaSolutions',  # Remove
        'SuccessMetrics'  # Remove
    ]
    
    # Companies with corrected URLs (verified working)
    url_corrections = {
        'Frontstep': 'https://jobs.lever.co/frontsteps',
        'XTN Cognitive Security': 'https://xtncognitivesecurity.com/careers/',
        'EventHub': 'https://join.com/companies/eventhubhq',
        'Symphony AI': 'https://symphony.com/company/careers/',
        'Brightwave': 'https://www.linkedin.com/company/brightwaveio/jobs/',
        'Huntington Bank': 'https://huntington-careers.com/search/searchjobs?radius=25',
        'TrackVia': 'https://job-boards.greenhouse.io/trackvia',
        'Turnkey': 'https://recruiting.paylocity.com/recruiting/jobs/All/b9426649-846b-43a6-9890-98c6c5c03eb4/Turnkey-Technologies-Inc',
        'Cove': 'https://careers.cove.is/',
        'REI Hub': 'https://www.reihub.net/careers/',
        'Molo Finance': 'https://www.linkedin.com/company/molofinance/jobs/',
        'Phase Change Software': 'https://phasechangesoftwarellc.applytojob.com/apply',
        'Gusto': 'https://gusto.com/about/careers/join-the-team'
    }
    
    # Companies with 404 errors - mark for job board searches only
    career_404s = [
        'AbsenceSoft', 'Advanced Fraud Solutions', 'DarkOwl', 
        'First Arriving', 'EVO Snap', 'Exterro', 'Call Center Studio',
        'Appfolio', 'Rentec Direct', 'Handbid', 'ResourceX', 'SaferWatch',
        'Selecthub', 'Lenda', 'Liqid', 'LiveAgent', 'Ushahidi',
        'LexisNexis Risk Solutions', 'Macrium Software', 'NICE Actimize',
        'Intranext Systems', 'Seon', 'Silent Eight', 'Precog',
        'PrintRelief', 'Phase Change Software', 'TurboTenent', 'Bacflip'
    ]
    
    # Companies with working sites but no current job postings (keep but flag)
    no_current_jobs = [
        'Macrium Software'  # Site is correct, just no jobs currently
    ]
    
    # New companies to add (AudienceView, Spektrix, Tessitura)
    new_companies = [
        {
            'Company': 'AudienceView',
            'Industry': 'Entertainment Technology', 
            'City': 'Toronto',
            'Careers URL': 'https://www.audienceview.com/careers/',
            'Company_Size': 'Medium',
            'Primary_Source': 'Manual_Addition',
            'Indeed Search': 'AudienceView product manager',
            'AngelList_URL': '',
            'Glassdoor_URL': '',
            'Backup_Strategy': 'job_boards'
        },
        {
            'Company': 'Spektrix',
            'Industry': 'Arts Technology',
            'City': 'London', 
            'Careers URL': 'https://www.spektrix.com/about-us/careers/',
            'Company_Size': 'Medium',
            'Primary_Source': 'Manual_Addition',
            'Indeed Search': 'Spektrix product manager',
            'AngelList_URL': '',
            'Glassdoor_URL': '',
            'Backup_Strategy': 'job_boards'
        },
        {
            'Company': 'Tessitura',
            'Industry': 'Arts Management Software',
            'City': 'New York',
            'Careers URL': 'https://www.tessituranetwork.com/about/careers',
            'Company_Size': 'Medium', 
            'Primary_Source': 'Manual_Addition',
            'Indeed Search': 'Tessitura product manager',
            'AngelList_URL': '',
            'Glassdoor_URL': '',
            'Backup_Strategy': 'job_boards'
        }
    ]
    
    try:
        # Load the original dataset
        df = pd.read_csv('companies_final_ready.csv')
        original_count = len(df)
        logging.info(f"📊 Processing {original_count} companies")
        
        # Create backup
        backup_filename = f"companies_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(backup_filename, index=False)
        logging.info(f"✅ Created backup: {backup_filename}")
        
        # Remove companies that should be deleted
        df_cleaned = df[~df['Company'].isin(companies_to_remove)]
        removed_count = original_count - len(df_cleaned)
        logging.info(f"🗑️ Removed {removed_count} companies")
        
        # Apply URL corrections
        updates_made = 0
        for company, new_url in url_corrections.items():
            mask = df_cleaned['Company'] == company
            if mask.any():
                old_url = df_cleaned.loc[mask, 'Careers URL'].iloc[0] if not df_cleaned.loc[mask, 'Careers URL'].empty else 'N/A'
                df_cleaned.loc[mask, 'Careers URL'] = new_url
                logging.info(f"🔧 Updated {company}: {old_url} → {new_url}")
                updates_made += 1
            else:
                logging.warning(f"⚠️ Company '{company}' not found in dataset")
        
        # Add status column
        df_cleaned['Status'] = 'Active'
        df_cleaned['Last_Updated'] = datetime.now().strftime('%Y-%m-%d')
        
        # Add new companies
        for new_company in new_companies:
            # Check if company already exists
            if not (df_cleaned['Company'] == new_company['Company']).any():
                new_row = pd.DataFrame([new_company])
                df_cleaned = pd.concat([df_cleaned, new_row], ignore_index=True)
                logging.info(f"➕ Added new company: {new_company['Company']}")
            else:
                logging.warning(f"⚠️ Company '{new_company['Company']}' already exists, skipping")
        
        # Flag companies with 404 errors for job board searches
        for company in career_404s:
            mask = df_cleaned['Company'] == company
            if mask.any():
                df_cleaned.loc[mask, 'Status'] = 'Job_Boards_Only'
                logging.info(f"🏷️ Flagged {company}: Job boards only (404 career page)")
        
        # Flag companies with no current jobs
        for company in no_current_jobs:
            mask = df_cleaned['Company'] == company
            if mask.any():
                df_cleaned.loc[mask, 'Status'] = 'No_Current_Jobs'
                logging.info(f"🏷️ Flagged {company}: No current jobs")
        
        # Save the cleaned dataset
        df_cleaned.to_csv('companies_corrected.csv', index=False)
        logging.info(f"✅ Saved corrected dataset: companies_corrected.csv")
        
        # Generate reports
        generate_correction_reports(df, df_cleaned, companies_to_remove, url_corrections)
        
        # Summary
        logging.info(f"\n📈 CORRECTION SUMMARY:")
        logging.info(f"   Original companies: {original_count}")
        logging.info(f"   Companies removed: {removed_count}")
        logging.info(f"   URLs updated: {updates_made}")
        logging.info(f"   New companies added: {len(new_companies)}")
        logging.info(f"   Final count: {len(df_cleaned)}")
        logging.info(f"   Active companies: {len(df_cleaned[df_cleaned['Status'] == 'Active'])}")
        
        return df_cleaned
        
    except FileNotFoundError:
        logging.error("❌ companies_final_ready.csv not found!")
        return None
    except Exception as e:
        logging.error(f"❌ Error during correction process: {e}")
        return None

def generate_correction_reports(original_df, cleaned_df, removed_companies, url_corrections):
    """Generate detailed reports about the corrections made"""
    
    # Report of removed companies
    removed_df = original_df[original_df['Company'].isin(removed_companies)]
    removed_df['Removal_Reason'] = 'Verified_Unusable'
    removed_df.to_csv('removed_companies_report.csv', index=False)
    
    # Report of URL updates
    updated_companies = []
    for company, new_url in url_corrections.items():
        mask = cleaned_df['Company'] == company
        if mask.any():
            row = cleaned_df[mask].iloc[0].copy()
            row['New_URL'] = new_url
            updated_companies.append(row)
    
    if updated_companies:
        updates_df = pd.DataFrame(updated_companies)
        updates_df.to_csv('url_updates_report.csv', index=False)
    
    # Generate markdown report
    report_content = f"""# Company Dataset Correction Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Original companies:** {len(original_df)}
- **Companies removed:** {len(removed_companies)}
- **URLs corrected:** {len(url_corrections)}
- **Final company count:** {len(cleaned_df)}

## Companies Removed
{chr(10).join([f"- {company}" for company in removed_companies])}

## URLs Corrected
{chr(10).join([f"- **{company}:** {url}" for company, url in url_corrections.items()])}

## Next Steps
1. Use `companies_corrected.csv` as your input file
2. The corrected URLs should significantly reduce scraping errors
3. Monitor for any remaining issues with the flagged companies
4. Companies marked as 'Job_Boards_Only' should use Indeed/LinkedIn searches

## Files Generated
- `companies_corrected.csv` - Main corrected dataset
- `removed_companies_report.csv` - Details of removed companies  
- `url_updates_report.csv` - Details of URL corrections
- `companies_backup_[timestamp].csv` - Backup of original data
"""
    
    with open('correction_report.md', 'w') as f:
        f.write(report_content)
    
    logging.info("📄 Generated detailed reports")

def validate_corrections():
    """Quick validation of the corrections"""
    try:
        df = pd.read_csv('companies_corrected.csv')
        
        # Check for duplicates
        duplicates = df[df.duplicated('Company', keep=False)]
        if not duplicates.empty:
            logging.warning(f"⚠️ Found {len(duplicates)} duplicate companies")
        
        # Check for empty URLs
        empty_urls = df[df['Careers URL'].isna() | (df['Careers URL'] == '')]
        if not empty_urls.empty:
            logging.warning(f"⚠️ Found {len(empty_urls)} companies with empty URLs")
        
        # Check status distribution
        status_counts = df['Status'].value_counts()
        logging.info(f"📊 Status distribution:")
        for status, count in status_counts.items():
            logging.info(f"   {status}: {count}")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Validation failed: {e}")
        return False

if __name__ == "__main__":
    logging.info("🚀 Starting company dataset correction...")
    
    # Apply corrections
    corrected_df = apply_corrections()
    
    if corrected_df is not None:
        # Validate the corrections
        if validate_corrections():
            logging.info("✅ Correction process completed successfully!")
            logging.info("📁 Use 'companies_corrected.csv' for your next scraping run")
        else:
            logging.error("❌ Validation failed - please review the corrections")
    else:
        logging.error("❌ Correction process failed")
