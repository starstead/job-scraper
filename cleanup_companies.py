import pandas as pd

def fix_and_clean_companies():
    """Fix broken URLs, remove duplicates, and prepare manual review list"""
    
    # Load the enhanced CSV
    df = pd.read_csv('companies_enhanced.csv')
    print(f"Loaded {len(df)} companies")
    
    # Step 1: Remove exact duplicates
    initial_count = len(df)
    df = df.drop_duplicates(subset=['Company'], keep='first')
    duplicates_removed = initial_count - len(df)
    print(f"Removed {duplicates_removed} exact duplicate companies")
    
    # Step 2: Find and handle similar company names
    print("\nChecking for similar company names...")
    similar_companies = []
    company_names = df['Company'].str.lower().str.strip()
    
    for i, name1 in enumerate(company_names):
        for j, name2 in enumerate(company_names):
            if i < j:  # Avoid comparing same pair twice
                # Check for very similar names (e.g., "DataDog" vs "Data Dog")
                name1_clean = name1.replace(' ', '').replace('.', '').replace('-', '')
                name2_clean = name2.replace(' ', '').replace('.', '').replace('-', '')
                
                if name1_clean == name2_clean and name1 != name2:
                    similar_companies.append((df.iloc[i]['Company'], df.iloc[j]['Company']))
    
    if similar_companies:
        print("Found similar company names:")
        for comp1, comp2 in similar_companies:
            print(f"  - '{comp1}' vs '{comp2}'")
        
        # Auto-merge obvious duplicates
        companies_to_remove = []
        for comp1, comp2 in similar_companies:
            if 'data dog' in comp1.lower() and 'datadog' in comp2.lower():
                companies_to_remove.append(comp1)  # Keep DataDog, remove "Data Dog"
            elif len(comp1) > len(comp2):  # Keep shorter name
                companies_to_remove.append(comp1)
            else:
                companies_to_remove.append(comp2)
        
        # Remove the duplicates
        df = df[~df['Company'].isin(companies_to_remove)]
        print(f"Auto-merged {len(companies_to_remove)} similar companies")
    
    # Step 3: URL fixes for companies that had errors
    url_fixes = {
        # 403 Forbidden - Updated URLs
        'A2ZSync': 'https://a2zsync.com/careers',
        'Airwallex': 'https://careers.airwallex.com/jobs',
        'AlertMedia': 'https://www.alertmedia.com/careers',
        'Alteryx': 'https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers',
        'Bombora': 'https://bombora.com/about/careers',
        'BurstIQ': 'https://burstiq.com/careers',
        'Carbyne': 'https://carbyne911.com/about/careers',
        'FluentStream': 'https://www.fluentstream.com/about/careers',
        'Fullcontact': 'https://www.fullcontact.com/about/careers',
        'Gig smart': 'https://www.gigsmart.com/about/careers',
        'Gusto': 'https://gusto.com/about/careers',
        'Mark43': 'https://www.mark43.com/about/careers',
        'Mitel': 'https://www.mitel.com/company/careers',
        'Pathify': 'https://pathify.com/about/careers',
        'Quizlet': 'https://quizlet.com/about/careers',
        'Sunbit': 'https://sunbit.com/about/careers',
        'Vendavo': 'https://www.vendavo.com/about/careers',
        'Seon': 'https://seon.io/about/careers',
        'EverCommerce': 'https://www.evercommerce.com/careers',
        
        # 404 Not Found - Corrected URLs
        'CQG': 'https://www.cqg.com/careers',
        'DataVisor': 'https://www.datavisor.com/company/careers',
        'Five9': 'https://www.five9.com/company/careers',
        'Healthgrades': 'https://careers.healthgrades.com',
        'Tyler Tech': 'https://careers.tylertech.com',
        'Ping Identity': 'https://www.pingidentity.com/careers',
        'TurboTenant': 'https://www.turbotenant.com/about/careers',
        'Appfolio': 'https://www.appfolio.com/about/careers',
        'Rentec Direct': 'https://www.rentecdirect.com/careers',
        
        # Major companies with known correct URLs
        'DataDog': 'https://www.datadoghq.com/careers',
        'Data Dog': 'https://www.datadoghq.com/careers',
        'Snowflake': 'https://careers.snowflake.com',
        'Palantir': 'https://www.palantir.com/careers',
        'Twilio': 'https://www.twilio.com/en-us/company/jobs',
        'Fastly': 'https://www.fastly.com/about/careers',
        'Proofpoint': 'https://www.proofpoint.com/us/company/careers',
        'FICO': 'https://www.fico.com/careers',
        'Fivetran': 'https://www.fivetran.com/careers',
        'UiPath': 'https://www.uipath.com/careers',
        'Genesys': 'https://www.genesys.com/careers',
        'Guild Education': 'https://www.guildeducation.com/careers',
        'Huntington Bank': 'https://www.huntington.com/careers',
        'Thomson Reuters': 'https://careers.thomsonreuters.com',
        'Kaseware': 'https://www.kaseware.com/careers',
        'L3Harris': 'https://careers.l3harris.com',
        'General Dynamics': 'https://gdmissionsystems.com/careers',
        'Hexagon': 'https://hexagon.com/careers',
        'Invisible Technologies': 'https://invisible.co/careers',
        'Trimble': 'https://careers.trimble.com',
        'Meltwater': 'https://www.meltwater.com/careers'
    }
    
    # Step 4: Apply URL fixes
    fixes_applied = 0
    for company_name, new_url in url_fixes.items():
        mask = df['Company'] == company_name
        if mask.any():
            old_url = df.loc[mask, 'Careers Site URL'].iloc[0]
            df.loc[mask, 'Careers Site URL'] = new_url
            df.loc[mask, 'Primary_Source'] = 'careers_page'
            print(f"Fixed: {company_name}")
            fixes_applied += 1
    
    print(f"\nApplied {fixes_applied} URL fixes")
    
    # Step 5: Identify companies still needing manual fixes
    companies_needing_manual_review = []
    
    for idx, row in df.iterrows():
        careers_url = row.get('Careers Site URL', '')
        company_name = row['Company']
        
        # Companies with missing URLs
        if pd.isna(careers_url) or not careers_url or careers_url == 'nan' or careers_url == '':
            companies_needing_manual_review.append({
                'Company': company_name,
                'Industry': row.get('Industry', ''),
                'City': row.get('City', ''),
                'Issue': 'Missing URL',
                'Suggested_Search': f"'{company_name} careers' in Google"
            })
        # Companies with LinkedIn URLs (should find company website instead)
        elif 'linkedin.com' in careers_url.lower():
            companies_needing_manual_review.append({
                'Company': company_name,
                'Industry': row.get('Industry', ''),
                'City': row.get('City', ''),
                'Issue': 'LinkedIn URL',
                'Current_URL': careers_url,
                'Suggested_Search': f"'{company_name} careers' in Google"
            })
    
    # Step 6: Create manual review CSV
    if companies_needing_manual_review:
        manual_df = pd.DataFrame(companies_needing_manual_review)
        manual_df['New_Careers_URL'] = ''  # Empty column for you to fill in
        manual_df['Action'] = ''  # Keep/Remove column
        manual_df.to_csv('companies_manual_review.csv', index=False)
        print(f"\nCreated manual review file: companies_manual_review.csv")
        print(f"Companies needing manual review: {len(companies_needing_manual_review)}")
        
        print(f"\nTop companies needing manual URLs:")
        for i, comp in enumerate(companies_needing_manual_review[:10]):
            print(f"  {i+1}. {comp['Company']} ({comp['Industry']}) - {comp['Issue']}")
    
    # Step 7: Clean and save the main CSV
    df = df.reset_index(drop=True)
    df.to_csv('companies_cleaned_final.csv', index=False)
    
    # Generate final summary
    print(f"\n=== FINAL CLEANUP SUMMARY ===")
    print(f"Original companies: {initial_count}")
    print(f"After removing duplicates: {len(df)}")
    print(f"URL fixes applied: {fixes_applied}")
    print(f"Companies needing manual review: {len(companies_needing_manual_review)}")
    
    # Count primary sources
    source_counts = df['Primary_Source'].value_counts()
    print(f"\nPrimary sources:")
    for source, count in source_counts.items():
        print(f"  {source}: {count} companies")
    
    # Company size breakdown
    size_counts = df['Company_Size'].value_counts()
    print(f"\nCompany sizes:")
    for size, count in size_counts.items():
        print(f"  {size}: {count} companies")
    
    print(f"\nFiles created:")
    print(f"  - companies_cleaned_final.csv (main file)")
    print(f"  - companies_manual_review.csv (for manual URL additions)")
    
    return df, companies_needing_manual_review

def apply_manual_fixes():
    """Apply manual fixes after user has filled in the manual review CSV"""
    try:
        # Load the manual review file
        manual_df = pd.read_csv('companies_manual_review.csv')
        main_df = pd.read_csv('companies_cleaned_final.csv')
        
        updates_applied = 0
        
        for _, row in manual_df.iterrows():
            company_name = row['Company']
            new_url = row.get('New_Careers_URL', '')
            action = row.get('Action', '').lower()
            
            if action == 'remove':
                # Remove company entirely
                main_df = main_df[main_df['Company'] != company_name]
                print(f"Removed: {company_name}")
                updates_applied += 1
            elif new_url and new_url != 'nan' and pd.notna(new_url):
                # Update with new URL
                mask = main_df['Company'] == company_name
                if mask.any():
                    main_df.loc[mask, 'Careers Site URL'] = new_url
                    main_df.loc[mask, 'Primary_Source'] = 'careers_page'
                    print(f"Updated: {company_name} -> {new_url}")
                    updates_applied += 1
        
        if updates_applied > 0:
            main_df.to_csv('companies_final_with_manual_fixes.csv', index=False)
            print(f"\nApplied {updates_applied} manual fixes")
            print("Saved: companies_final_with_manual_fixes.csv")
        else:
            print("No manual fixes found to apply")
            
    except FileNotFoundError:
        print("Manual review file not found. Complete the manual review first.")

if __name__ == "__main__":
    df, manual_review_list = fix_and_clean_companies()
    
    print(f"\n=== NEXT STEPS ===")
    print("1. Review the file: companies_manual_review.csv")
    print("2. For each company:")
    print("   - Add the correct careers URL in 'New_Careers_URL' column")
    print("   - OR put 'remove' in 'Action' column to delete the company")
    print("3. Save the file and run: apply_manual_fixes()")
    print("4. Then we'll create the multi-platform scraper!")
