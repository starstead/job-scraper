import pandas as pd

# Load files
df = pd.read_csv('companies_cleaned_final.csv')

# Apply your 4 URL updates
url_updates = {
    'Casebuilder': 'https://www.soundthinking.com/careers/',
    'emite': 'https://www.emite.com/careers/',
    'Highwing': 'https://www.highwing.io/careers',
    'i2 Suite': 'https://harriscomputer.wd3.myworkdayjobs.com/en-US/1?q=i2'
}

# Set these companies to use job boards instead of careers pages
job_board_companies = [
    'Absio', 'Advanced Fraud Solutions', 'Alpha Pro Tech LTD.', 
    'AssetSense', 'Brightwave', 'Call Center Studio', 
    'Catalyst Healthcare', 'Comtrac', 'CrossTrax'
]

# Apply URL updates
for company, url in url_updates.items():
    mask = df['Company'] == company
    if mask.any():
        df.loc[mask, 'Careers Site URL'] = url
        df.loc[mask, 'Primary_Source'] = 'careers_page'
        print(f"Updated {company}")

# Set job board companies
for company in job_board_companies:
    mask = df['Company'] == company
    if mask.any():
        df.loc[mask, 'Careers Site URL'] = ''
        df.loc[mask, 'Primary_Source'] = 'job_boards'
        print(f"Set {company} to job boards")

# Save final file
df.to_csv('companies_final_ready.csv', index=False)
print(f"\n✅ Final file ready: companies_final_ready.csv")
print(f"Total companies: {len(df)}")

# Show breakdown
source_counts = df['Primary_Source'].value_counts()
print(f"\nPrimary sources:")
for source, count in source_counts.items():
    print(f"  {source}: {count} companies")
