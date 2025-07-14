import pandas as pd
import requests
from urllib.parse import urlparse, urljoin
import time
import re

def clean_url(url):
    """Clean and normalize URL"""
    if pd.isna(url) or url == 'nan' or not url:
        return None
    
    url = str(url).strip()
    
    # Fix common issues
    if not url.startswith('http'):
        url = 'https://' + url
    
    # Remove duplicated URLs (like the concatenated one we saw)
    if 'https://close.com/jobs/' in url and url != 'https://close.com/jobs/':
        url = 'https://close.com/jobs/'
    
    return url

def find_careers_page(company_name, base_url=None):
    """Try to find working careers page for a company"""
    if not base_url:
        # Try to guess company website
        company_clean = company_name.lower().replace(' ', '').replace('.', '').replace(',', '')
        potential_sites = [
            f"https://{company_clean}.com",
            f"https://www.{company_clean}.com",
            f"https://{company_clean}.io",
            f"https://{company_clean}.ai"
        ]
    else:
        # Extract base domain from existing URL
        parsed = urlparse(base_url)
        base_domain = f"{parsed.scheme}://{parsed.netloc}"
        potential_sites = [base_domain]
    
    # Common careers page patterns
    careers_patterns = [
        "/careers",
        "/careers/",
        "/jobs", 
        "/jobs/",
        "/about/careers",
        "/company/careers",
        "/join-us",
        "/work-with-us",
        "/team/careers",
        "/careers-jobs"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for base in potential_sites:
        for pattern in careers_patterns:
            try:
                test_url = base + pattern
                response = requests.head(test_url, headers=headers, timeout=5, allow_redirects=True)
                
                if response.status_code == 200:
                    print(f"✅ Found: {company_name} -> {test_url}")
                    return test_url
                    
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                continue
    
    print(f"❌ No careers page found for: {company_name}")
    return None

def get_company_fixes():
    """Manual fixes for known companies"""
    return {
        # Companies with known correct URLs
        'Clearview AI': 'https://www.clearview.ai/careers',
        'Lenda': 'https://www.lenda.com/careers',
        'A.L.E.I.R': '',  # Remove - no careers page
        
        # Updated URLs for companies that moved
        'Close': 'https://close.com/jobs/',
        'Dataiku': 'https://www.dataiku.com/careers/',
        'Experian': 'https://www.experianplc.com/careers/',
        'Healthgrades': 'https://www.healthgrades.com/careers/',
        'EverBridge': 'https://www.everbridge.com/company/careers/',
        'Five9': 'https://www.five9.com/company/careers',
        'Fivetran': 'https://www.fivetran.com/careers',
        'FICO': 'https://www.fico.com/en/careers',
        'Proofpoint': 'https://www.proofpoint.com/us/company/careers',
        'Snowflake': 'https://careers.snowflake.com/',
        'Twilio': 'https://www.twilio.com/en-us/company/jobs',
        'UiPath': 'https://www.uipath.com/careers',
        'Tyler Tech': 'https://www.tylertech.com/about/careers',
        'User Testing': 'https://www.usertesting.com/company/careers',
        'DataDog': 'https://www.datadoghq.com/careers/',
        'Data Dog': 'https://www.datadoghq.com/careers/',
        'Palantir': 'https://www.palantir.com/careers/',
        'Alteryx': 'https://www.alteryx.com/careers',
        'BiggerPockets': 'https://www.biggerpockets.com/careers',
        'Fastly': 'https://www.fastly.com/about/careers',
        'Genesys': 'https://www.genesys.com/company/careers',
        'Granicus': 'https://granicus.com/careers/',
        'Guild Education': 'https://www.guildeducation.com/careers/',
        'Huntington Bank': 'https://www.huntington.com/careers',
        'Thomson Reuters': 'https://careers.thomsonreuters.com/',
        'Trimble': 'https://careers.trimble.com/',
        'Invisible Technologies': 'https://invisible.co/careers',
        'Kaseware': 'https://www.kaseware.com/careers/',
        'Kiva': 'https://www.kiva.org/careers',
        'L3Harris': 'https://careers.l3harris.com/',
        'Lightspeed': 'https://www.lightspeedhq.com/careers/',
        'General Dynamics': 'https://gdmissionsystems.com/careers',
        'Genetec': 'https://www.genetec.com/careers',
        'GHX': 'https://www.ghx.com/about/careers/',
        'Hexagon': 'https://hexagon.com/careers',
        'Meltwater': 'https://www.meltwater.com/careers/',
        'Model N': 'https://www.modeln.com/company/careers/',
        'Musiquest': 'https://www.musiquest.com/about/careers/',
        'Ping Identity': 'https://www.pingidentity.com/en/careers.html',
        'Planning Pod': 'https://www.planningpod.com/company/careers.aspx',
        'Poppulo': 'https://www.poppulo.com/careers/',
        'Power Takeoff': 'https://www.powertakeoff.com/about/careers/',
        'Prime Pay': 'https://www.primepay.com/careers/',
        'Procare Solutions': 'https://www.procaresoftware.com/careers/',
        'Quantum Metric': 'https://quantummetric.com/careers/',
        'Quizlet': 'https://quizlet.com/careers',
        'Red Canary': 'https://redcanary.com/careers/',
        'Redeam': 'https://www.redeam.com/careers/',
        'SaferWatch': 'https://www.saferwatch.com/careers/',
        'Sardine': 'https://www.sardine.ai/careers/',
        'Seequent': 'https://www.seequent.com/careers/',
        'Serve robotics': 'https://www.serverobotics.com/careers/',
        'Siemens': 'https://jobs.siemens.com/careers',
        'SmartWyre': 'https://www.smartwyre.com/careers/',
        'Softheon': 'https://www.softheon.com/careers/',
        'Sondermind': 'https://www.sondermind.com/careers/',
        'Spekit': 'https://www.spekit.com/careers/',
        'Switch Automation': 'https://www.switchautomation.com/careers/',
        'TalkDesk': 'https://www.talkdesk.com/careers/',
        'TaskRay': 'https://taskray.com/careers/',
        'TeKnowledge': 'https://www.teknowledge.com/careers/',
        'Tenemos': 'https://www.temenos.com/careers/',
        'The Receptionist': 'https://thereceptionist.com/careers/',
        'TrackVia': 'https://trackvia.com/careers/',
        'Ttec': 'https://www.ttec.com/careers',
        'UJET': 'https://ujet.com/careers/',
        'Upgrade': 'https://upgrade.com/careers/',
        'Upland RO Innovation': 'https://uplandsoftware.com/careers/',
        'Vendavo': 'https://www.vendavo.com/careers/',
        'Versaterm': 'https://www.versaterm.com/careers/',
        'Wing (by Alphabet)': 'https://wing.com/careers/',
        'Xanalys': 'https://www.xanalys.com/careers/',
        'Xata': 'https://xata.io/careers',
        'Mambu': 'https://www.mambu.com/careers/',
        'Sift': 'https://sift.com/careers/',
        'Replicant': 'https://www.replicant.ai/careers/',
        'Snapdocs': 'https://www.snapdocs.com/careers/',
        'Cove': 'https://www.cove.com/careers/',
        'CertifiD': 'https://www.certifid.com/careers/',
        'Anywhere Real Estate Inc.': 'https://anywhere.re/careers/',
        'Roofr': 'https://www.roofr.com/careers/',
        'Opendoor': 'https://www.opendoor.com/careers/',
        'Appfolio': 'https://www.appfolio.com/careers/',
        'F5': 'https://www.f5.com/company/careers',
        'Silent Eight': 'https://silenteight.com/careers/',
        'Arctic Intelligence': 'https://www.arctic-intelligence.com/careers/',
        'Symphony AI': 'https://www.symphony.ai/careers/',
        'Truid': 'https://www.truid.app/careers/',
        'Ocrolus': 'https://www.ocrolus.com/careers/',
        'Sagitec': 'https://www.sagitec.com/careers/',
        'CQG': 'https://www.cqg.com/about/careers',
        'Five9': 'https://www.five9.com/company/careers',
        'Exterro': 'https://www.exterro.com/company/careers/',
        'FareHarbor': 'https://fareharbor.com/careers/',
        'Filevine': 'https://www.filevine.com/careers/',
        'Freshworks': 'https://www.freshworks.com/company/careers/',
        'Management Controls': 'https://managementcontrols.com/careers/',
        'MeetMonk': 'https://meetmonk.com/careers/',
        'TurboTenant': 'https://www.turbotenant.com/careers/',
        'Versaterm': 'https://www.versaterm.com/careers/',
        'EverCommerce': 'https://www.evercommerce.com/about/careers/',
        'Feedzai': 'https://careers.feedzai.com/',
        'FinFolio': 'https://www.finfolio.com/careers/',
        'Mark43': 'https://www.mark43.com/careers/',
        'Maxwell': 'https://www.hellobetter.com/careers/',
        'Mersive': 'https://mersive.com/careers/',
        'Mitel': 'https://www.mitel.com/company/careers',
        'Molo Finance': 'https://www.molofinance.com/careers/',
        'mPulse': 'https://www.mpulse.com/careers/',
        'Navigator Business Solutions': 'https://www.nbs-us.com/careers/',
        'Netstock': 'https://www.netstock.com/careers/',
        'NICE Actimize': 'https://www.niceactimize.com/careers/',
        'Nymbl Science': 'https://nymblscience.com/careers/',
        'Ombud': 'https://ombud.com/careers/',
        'Omnigo': 'https://www.omnigo.com/careers/',
        'Open Door': 'https://www.opendoor.com/careers/',
        'Orderly': 'https://www.orderly.com/careers/',
        'OrthoFi': 'https://orthofi.com/careers/',
        'Pairin': 'https://pairin.com/careers/',
        'Pathify': 'https://pathify.com/careers/',
        'Phase Change Software': 'https://phasechangesoftware.com/careers/',
        'Planet Labs': 'https://www.planet.com/careers/',
        'Precog': 'https://precog.com/careers/',
        'PrintRelief': 'https://printrelief.com/careers/',
        'Reali Solutions': 'https://reali.com/careers/',
        'ResourceX': 'https://resourcex.com/careers/',
        'SAS® Law Enforcement Intelligence': 'https://www.sas.com/en_us/careers.html',
        'Selecthub': 'https://selecthub.com/careers/',
        'Sunbit': 'https://sunbit.com/careers/',
        'Truu': 'https://truu.ai/careers/',
        'Turnkey': 'https://www.turnkeytechnologies.com/careers/',
        'Ushahidi': 'https://www.ushahidi.com/careers/',
        'Utility Inc.': 'https://utility.com/careers/',
        'XTN Cognitive Security': 'https://xtn-labs.com/careers/',
        'Bacflip': 'https://www.backflip.com/careers/',
        'Rentec Direct': 'https://www.rentecdirect.com/about/careers.aspx',
        'Seon': 'https://seon.io/careers/',
        'Empower': 'https://www.empower.me/careers',
        'Epsilon': 'https://www.epsilon.com/us/careers',
        'Documoto': 'https://documoto.com/careers/',
        'Elastic Suite': 'https://www.elastic.co/careers/',
        'EVO Snap': 'https://evosnap.com/careers/',
        'FluoroFinder': 'https://www.fluorofinder.com/careers/',
        'Flytedesk': 'https://flytedesk.com/careers/',
        'Gogo Business Aviation': 'https://jobs.jobvite.com/gogo/',
        'Gridics': 'https://gridics.com/careers/',
        'Handbid': 'https://www.handbid.com/careers/',
        'IQware': 'https://www.iqware.com/careers/',
        'Josh.ai': 'https://josh.ai/careers/',
        'Kantox': 'https://kantox.com/careers/',
        'Kologik': 'https://www.kologik.com/careers/',
        'Lendesk': 'https://lendesk.com/careers/',
        'LexisNexis Risk Solutions': 'https://risk.lexisnexis.com/careers',
        'Liqid': 'https://liqid.com/careers/',
        'LiveAgent': 'https://www.liveagent.com/careers/',
        'Lone Wolf': 'https://lonewolftechnologies.com/careers/',
        'Macrium Software': 'https://www.macrium.com/careers',
        'Magic Leap': 'https://www.magicleap.com/careers',
        'Magnet Forensics': 'https://www.magnetforensics.com/careers/',
        'Matrix': 'https://www.matrix-solutions.com/careers/',
        'Neat Capital': 'https://neat.capital/careers/',
        'Hosify': 'https://hosify.com/careers/',
        'REI Hub': 'https://reihub.com/careers/'
    }

def cleanup_company_urls():
    """Main function to clean up company URLs"""
    # Load current companies
    df = pd.read_csv('companies.csv')
    
    print(f"Loaded {len(df)} companies")
    
    # Get manual fixes
    known_fixes = get_company_fixes()
    
    # Apply manual fixes first
    for company, new_url in known_fixes.items():
        mask = df['Company'] == company
        if mask.any():
            df.loc[mask, 'Careers Site URL'] = new_url
            print(f"Fixed: {company} -> {new_url}")
    
    # Find and fix missing/broken URLs
    problematic_companies = df[
        (df['Careers Site URL'].isna()) | 
        (df['Careers Site URL'] == 'nan') |
        (df['Careers Site URL'] == '')
    ]
    
    print(f"\nFound {len(problematic_companies)} companies with missing URLs")
    
    for idx, row in problematic_companies.iterrows():
        company_name = row['Company']
        print(f"Searching for careers page: {company_name}")
        
        new_url = find_careers_page(company_name)
        if new_url:
            df.loc[idx, 'Careers Site URL'] = new_url
        
        time.sleep(1)  # Rate limiting
    
    # Clean up remaining URLs
    df['Careers Site URL'] = df['Careers Site URL'].apply(clean_url)
    
    # Remove companies with no careers page
    df_cleaned = df[df['Careers Site URL'].notna() & (df['Careers Site URL'] != '')]
    
    print(f"\n=== CLEANUP SUMMARY ===")
    print(f"Original companies: {len(df)}")
    print(f"Companies with valid URLs: {len(df_cleaned)}")
    print(f"Companies removed: {len(df) - len(df_cleaned)}")
    
    # Save cleaned file
    df_cleaned.to_csv('companies_cleaned.csv', index=False)
    print(f"\nSaved cleaned file: companies_cleaned.csv")
    
    # Show companies that were removed
    removed = df[~df.index.isin(df_cleaned.index)]
    if len(removed) > 0:
        print(f"\nRemoved companies (no valid careers page):")
        for company in removed['Company'].tolist():
            print(f"  - {company}")
    
    return df_cleaned

if __name__ == "__main__":
    cleanup_company_urls()
