import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime
import os

class CompanyResearchScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # Research categories based on your profiles
        self.research_categories = {
            'anne_focus': {
                'arts_tech': ['spektrix', 'audienceview', 'tessitura', 'patron manager', 'arts management software'],
                'event_tech': ['eventbrite', 'cvent', 'bizzabo', 'live events platform', 'ticketing software'],
                'cultural_institutions': ['museum software', 'theater management', 'orchestra management'],
                'higher_ed_events': ['campus events', 'university events', 'student engagement platform']
            },
            'alessandro_focus': {
                'global_fintech': ['international payments', 'global money transfer', 'cross-border payments'],
                'european_connected': ['EU operations', 'european expansion', 'GDPR compliance'],
                'public_safety_intl': ['international public safety', 'global emergency services'],
                'defense_global': ['defense contractors', 'military technology', 'international defense']
            }
        }
    
    def research_built_in_colorado(self):
        """Research Denver/Colorado tech companies"""
        print("🏔️ Researching Built In Colorado companies...")
        
        try:
            # Built In Colorado company listings
            url = "https://builtin.com/colorado/companies"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                companies = []
                
                # Look for company listings
                company_elements = soup.find_all(['div', 'article'], class_=re.compile(r'company|listing', re.I))
                
                for element in company_elements[:20]:  # Limit for testing
                    try:
                        # Extract company name
                        name_elem = element.find(['h2', 'h3', 'a'], class_=re.compile(r'company.*name|title', re.I))
                        company_name = name_elem.get_text().strip() if name_elem else "Unknown"
                        
                        # Extract description
                        desc_elem = element.find(['p', 'div'], class_=re.compile(r'description|summary', re.I))
                        description = desc_elem.get_text().strip() if desc_elem else ""
                        
                        # Extract company URL if available
                        link_elem = element.find('a', href=True)
                        company_url = link_elem['href'] if link_elem else ""
                        
                        if len(company_name) > 3 and company_name != "Unknown":
                            companies.append({
                                'company_name': company_name,
                                'description': description[:200],
                                'source': 'Built In Colorado',
                                'company_url': company_url,
                                'research_date': datetime.now().strftime('%Y-%m-%d')
                            })
                    
                    except Exception as e:
                        continue
                
                print(f"Found {len(companies)} Colorado companies")
                return companies
            
        except Exception as e:
            print(f"Error researching Built In Colorado: {e}")
        
        return []
    
    def research_arts_tech_companies(self):
        """Research companies in arts/cultural technology space"""
        print("🎭 Researching arts & cultural technology companies...")
        
        companies = []
        
        # Known competitors/adjacent to Spektrix, AudienceView, Tessitura
        arts_tech_companies = [
            {
                'company_name': 'PatronManager',
                'description': 'CRM and fundraising software for arts organizations',
                'industry': 'Arts Technology',
                'focus': 'anne_focus'
            },
            {
                'company_name': 'Blackbaud',
                'description': 'Nonprofit and arts organization management software',
                'industry': 'Nonprofit Technology', 
                'focus': 'anne_focus'
            },
            {
                'company_name': 'Eventbrite',
                'description': 'Event management and ticketing platform',
                'industry': 'Event Technology',
                'focus': 'anne_focus'
            },
            {
                'company_name': 'Zettle (by PayPal)',
                'description': 'Point of sale for events and small businesses',
                'industry': 'Event Technology',
                'focus': 'anne_focus'
            },
            {
                'company_name': 'Cvent',
                'description': 'Event management platform for enterprises',
                'industry': 'Event Technology',
                'focus': 'anne_focus'
            },
            {
                'company_name': 'ACTIVE Network',
                'description': 'Registration and management for events and activities',
                'industry': 'Event Technology',
                'focus': 'anne_focus'
            }
        ]
        
        for company in arts_tech_companies:
            company['source'] = 'Arts Tech Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
            companies.append(company)
        
        print(f"Found {len(companies)} arts tech companies")
        return companies
    
    def research_global_fintech_companies(self):
        """Research fintech companies with global/European connections"""
        print("🌍 Researching global fintech companies...")
        
        companies = []
        
        # Companies with strong European/international focus
        global_fintech_companies = [
            {
                'company_name': 'Wise (formerly TransferWise)',
                'description': 'International money transfers and multi-currency accounts',
                'industry': 'Global Fintech',
                'focus': 'alessandro_focus',
                'why_relevant': 'Strong European roots, multilingual operations'
            },
            {
                'company_name': 'Remitly',
                'description': 'International money transfer to emerging markets',
                'industry': 'Global Fintech',
                'focus': 'alessandro_focus',
                'why_relevant': 'Global compliance, AML expertise needed'
            },
            {
                'company_name': 'Checkout.com',
                'description': 'Global payment processing platform',
                'industry': 'Global Fintech',
                'focus': 'alessandro_focus',
                'why_relevant': 'EU operations, regulatory compliance'
            },
            {
                'company_name': 'Stripe',
                'description': 'Global payment infrastructure',
                'industry': 'Global Fintech',
                'focus': 'alessandro_focus',
                'why_relevant': 'International expansion, EU compliance'
            },
            {
                'company_name': 'Klarna',
                'description': 'Swedish buy-now-pay-later, US expansion',
                'industry': 'Global Fintech',
                'focus': 'alessandro_focus',
                'why_relevant': 'European company expanding in US'
            }
        ]
        
        for company in global_fintech_companies:
            company['source'] = 'Global Fintech Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
            companies.append(company)
        
        print(f"Found {len(companies)} global fintech companies")
        return companies
    
    def research_public_safety_companies(self):
        """Research public safety and emergency services companies"""
        print("🚨 Researching public safety technology companies...")
        
        companies = []
        
        # Companies in public safety space (relevant to Alessandro's Motorola experience)
        public_safety_companies = [
            {
                'company_name': 'Tyler Technologies',
                'description': 'Software for state and local government including public safety',
                'industry': 'Public Safety Technology',
                'focus': 'alessandro_focus'
            },
            {
                'company_name': 'Genetec',
                'description': 'Security and public safety software platforms',
                'industry': 'Public Safety Technology', 
                'focus': 'alessandro_focus'
            },
            {
                'company_name': 'Axon',
                'description': 'Body cameras and evidence management for law enforcement',
                'industry': 'Public Safety Technology',
                'focus': 'alessandro_focus'
            },
            {
                'company_name': 'Palantir',
                'description': 'Data analytics for government and defense',
                'industry': 'Government Technology',
                'focus': 'alessandro_focus'
            },
            {
                'company_name': 'Mark43',
                'description': 'Cloud-based software for law enforcement',
                'industry': 'Public Safety Technology',
                'focus': 'alessandro_focus'
            }
        ]
        
        for company in public_safety_companies:
            company['source'] = 'Public Safety Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
            companies.append(company)
        
        print(f"Found {len(companies)} public safety companies")
        return companies
    
    def research_y_combinator_companies(self):
        """Research Y Combinator companies relevant to your interests"""
        print("🚀 Researching Y Combinator companies...")
        
        companies = []
        
        # YC companies that match your profiles
        yc_companies = [
            {
                'company_name': 'Stripe',
                'description': 'Online payment processing for internet businesses',
                'industry': 'Fintech',
                'focus': 'alessandro_focus',
                'why_relevant': 'Global payments, European operations'
            },
            {
                'company_name': 'Checkr',
                'description': 'Background checks and identity verification',
                'industry': 'HR Technology',
                'focus': 'alessandro_focus', 
                'why_relevant': 'Compliance, identity verification, international'
            },
            {
                'company_name': 'Gitlab',
                'description': 'DevOps platform for software development',
                'industry': 'Developer Tools',
                'focus': 'both',
                'why_relevant': 'Global remote company, product management roles'
            },
            {
                'company_name': 'PlanetScale',
                'description': 'Database platform for developers',
                'industry': 'Developer Tools',
                'focus': 'both',
                'why_relevant': 'Growing product team, technical product roles'
            },
            {
                'company_name': 'PostHog',
                'description': 'Product analytics and feature flags',
                'industry': 'Analytics',
                'focus': 'anne_focus',
                'why_relevant': 'Product analytics, user research focus'
            }
        ]
        
        for company in yc_companies:
            company['source'] = 'Y Combinator Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
            companies.append(company)
        
        print(f"Found {len(companies)} Y Combinator companies")
        return companies
    
    def research_acquisition_targets(self):
        """Research companies that might be acquisition targets (like Skye/Comcast example)"""
        print("🎯 Researching acquisition-relevant companies...")
        
        companies = []
        
        # Companies with European connections or acquisition potential
        acquisition_relevant = [
            {
                'company_name': 'N26',
                'description': 'German digital bank expanding globally',
                'industry': 'Digital Banking',
                'focus': 'alessandro_focus',
                'why_relevant': 'German company, might need US expansion expertise'
            },
            {
                'company_name': 'Revolut',
                'description': 'UK digital bank and financial services',
                'industry': 'Digital Banking',
                'focus': 'alessandro_focus',
                'why_relevant': 'European fintech expanding to US'
            },
            {
                'company_name': 'Bunq',
                'description': 'Dutch digital bank',
                'industry': 'Digital Banking', 
                'focus': 'alessandro_focus',
                'why_relevant': 'European digital bank, potential US expansion'
            },
            {
                'company_name': 'SumUp',
                'description': 'European payment solutions company',
                'industry': 'Fintech',
                'focus': 'alessandro_focus',
                'why_relevant': 'European payments company with global ambitions'
            },
            {
                'company_name': 'GoCardless',
                'description': 'UK-based recurring payments platform',
                'industry': 'Fintech',
                'focus': 'alessandro_focus',
                'why_relevant': 'European fintech with US operations'
            }
        ]
        
        for company in acquisition_relevant:
            company['source'] = 'Acquisition Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
            companies.append(company)
        
        print(f"Found {len(companies)} acquisition-relevant companies")
        return companies
    
    def research_denver_tech_ecosystem(self):
        """Deep dive into Denver/Colorado tech ecosystem"""
        print("🏔️ Deep dive into Denver tech ecosystem...")
        
        companies = []
        
        # Known Denver tech companies relevant to your skills
        denver_companies = [
            {
                'company_name': 'SendGrid',
                'description': 'Email delivery service (now part of Twilio)',
                'industry': 'Communications',
                'focus': 'both',
                'location': 'Denver, CO'
            },
            {
                'company_name': 'Ibotta',
                'description': 'Cash back and rewards app',
                'industry': 'Consumer Technology',
                'focus': 'anne_focus',
                'location': 'Denver, CO'
            },
            {
                'company_name': 'Foundry',
                'description': 'Industrial IoT and data analytics',
                'industry': 'Industrial Technology',
                'focus': 'alessandro_focus',
                'location': 'Boulder, CO'
            },
            {
                'company_name': 'Shopify Plus',
                'description': 'E-commerce platform (Denver office)',
                'industry': 'E-commerce',
                'focus': 'both',
                'location': 'Denver, CO'
            },
            {
                'company_name': 'Confluent',
                'description': 'Data streaming platform (Denver office)',
                'industry': 'Data Infrastructure',
                'focus': 'both',
                'location': 'Denver, CO'
            },
            {
                'company_name': 'Ping Identity',
                'description': 'Identity and access management',
                'industry': 'Cybersecurity',
                'focus': 'alessandro_focus',
                'location': 'Denver, CO'
            }
        ]
        
        for company in denver_companies:
            company['source'] = 'Denver Tech Ecosystem'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
            companies.append(company)
        
        print(f"Found {len(companies)} Denver tech companies")
        return companies
    
    def validate_company_careers_page(self, company_name, company_url=None):
        """Attempt to find and validate company careers page"""
        if not company_url:
            # Try common patterns
            company_slug = company_name.lower().replace(' ', '').replace(',', '').replace('.', '')
            potential_urls = [
                f"https://{company_slug}.com/careers",
                f"https://www.{company_slug}.com/careers",
                f"https://{company_slug}.com/jobs",
                f"https://careers.{company_slug}.com"
            ]
        else:
            base_url = company_url.rstrip('/')
            potential_urls = [
                f"{base_url}/careers",
                f"{base_url}/jobs",
                f"{base_url}/careers/",
                f"{base_url}/about/careers"
            ]
        
        for url in potential_urls:
            try:
                response = self.session.get(url, timeout=5)
                if response.status_code == 200:
                    # Basic check if it looks like a careers page
                    content = response.text.lower()
                    if any(word in content for word in ['job', 'career', 'position', 'hiring', 'apply']):
                        return url
            except:
                continue
        
        return None
    
    def compile_research_results(self):
        """Compile all research into a comprehensive list"""
        print("🔍 Starting comprehensive company research...")
        
        all_companies = []
        
        # Run all research methods
        research_methods = [
            self.research_built_in_colorado,
            self.research_arts_tech_companies,
            self.research_global_fintech_companies, 
            self.research_public_safety_companies,
            self.research_y_combinator_companies,
            self.research_acquisition_targets,
            self.research_denver_tech_ecosystem
        ]
        
        for method in research_methods:
            try:
                companies = method()
                all_companies.extend(companies)
                time.sleep(2)  # Rate limiting
            except Exception as e:
                print(f"Error in {method.__name__}: {e}")
                continue
        
        # Add careers page validation
        print("🔗 Validating careers pages...")
        for company in all_companies:
            careers_url = self.validate_company_careers_page(
                company['company_name'], 
                company.get('company_url')
            )
            company['careers_url'] = careers_url or "Not found"
            company['careers_validated'] = careers_url is not None
        
        return all_companies
    
    def save_research_results(self, companies):
        """Save research results to CSV for review and integration"""
        
        if not companies:
            print("No companies found to save")
            return
        
        # Create DataFrame
        df = pd.DataFrame(companies)
        
        # Add columns to match your existing scraper format
        df['Company'] = df['company_name']
        df['Careers Site URL'] = df['careers_url']
        df['Industry'] = df.get('industry', 'Unknown')
        df['Primary_Source'] = 'careers_page'
        df['Company_Size'] = 'Medium'  # Default, can be researched further
        df['City'] = 'Denver'  # Default for now
        
        # Save results
        os.makedirs('research_results', exist_ok=True)
        
        # Save detailed research
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        detailed_file = f'research_results/company_research_{timestamp}.csv'
        df.to_csv(detailed_file, index=False)
        
        # Save ready-to-integrate format
        integration_columns = ['Company', 'Careers Site URL', 'Industry', 'Primary_Source', 'Company_Size', 'City']
        integration_df = df[integration_columns].copy()
        
        # Filter only companies with valid careers pages
        valid_companies = integration_df[df['careers_validated'] == True]
        
        integration_file = f'research_results/new_companies_to_integrate_{timestamp}.csv'
        valid_companies.to_csv(integration_file, index=False)
        
        # Create summary
        summary = {
            'total_companies_researched': len(companies),
            'companies_with_careers_pages': len(valid_companies),
            'anne_focused_companies': len([c for c in companies if c.get('focus') in ['anne_focus', 'both']]),
            'alessandro_focused_companies': len([c for c in companies if c.get('focus') in ['alessandro_focus', 'both']]),
            'research_date': datetime.now().isoformat(),
            'files_created': [detailed_file, integration_file]
        }
        
        summary_file = f'research_results/research_summary_{timestamp}.json'
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n📊 RESEARCH COMPLETE!")
        print(f"Total companies researched: {summary['total_companies_researched']}")
        print(f"Companies with valid careers pages: {summary['companies_with_careers_pages']}")
        print(f"Anne-focused companies: {summary['anne_focused_companies']}")
        print(f"Alessandro-focused companies: {summary['alessandro_focused_companies']}")
        print(f"\nFiles created:")
        print(f"  - Detailed research: {detailed_file}")
        print(f"  - Integration ready: {integration_file}")
        print(f"  - Summary: {summary_file}")
        
        return valid_companies

if __name__ == "__main__":
    researcher = CompanyResearchScraper()
    companies = researcher.compile_research_results()
    researcher.save_research_results(companies)
