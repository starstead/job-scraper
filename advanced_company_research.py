import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime
import os

class AdvancedCompanyResearcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
    def scrape_built_in_companies(self, location="colorado"):
        """Scrape Built In website for tech companies"""
        print(f"🌐 Scraping Built In {location.title()} for companies...")
        
        companies = []
        try:
            url = f"https://builtin.com/{location}/companies"
            
            for page in range(1, 4):  # Scrape first 3 pages
                page_url = f"{url}?page={page}" if page > 1 else url
                print(f"  Scraping page {page}...")
                
                response = self.session.get(page_url, timeout=10)
                if response.status_code != 200:
                    continue
                    
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for company cards/listings
                company_cards = soup.find_all(['div', 'article'], class_=re.compile(r'company|card', re.I))
                
                for card in company_cards:
                    try:
                        # Extract company name
                        name_elem = card.find(['h2', 'h3', 'a'], class_=re.compile(r'company.*name|title', re.I))
                        if not name_elem:
                            name_elem = card.find('a', href=re.compile(r'/company/'))
                        
                        if name_elem:
                            company_name = name_elem.get_text().strip()
                            
                            # Extract description
                            desc_elem = card.find(['p', 'div'], class_=re.compile(r'description|summary', re.I))
                            description = desc_elem.get_text().strip() if desc_elem else ""
                            
                            # Extract company URL
                            link_elem = card.find('a', href=True)
                            company_page = link_elem['href'] if link_elem else ""
                            
                            # Try to extract industry/category
                            industry_elem = card.find(['span', 'div'], class_=re.compile(r'industry|category|tag', re.I))
                            industry = industry_elem.get_text().strip() if industry_elem else "Technology"
                            
                            if len(company_name) > 2 and company_name.lower() not in ['more', 'view', 'see']:
                                companies.append({
                                    'company_name': company_name,
                                    'description': description[:300],
                                    'industry': industry,
                                    'source': f'Built In {location.title()}',
                                    'company_page': company_page,
                                    'location': location.title(),
                                    'research_date': datetime.now().strftime('%Y-%m-%d')
                                })
                    
                    except Exception as e:
                        continue
                
                time.sleep(2)  # Rate limiting
                
        except Exception as e:
            print(f"Error scraping Built In {location}: {e}")
        
        print(f"  Found {len(companies)} companies from Built In {location.title()}")
        return companies
    
    def research_arts_tech_ecosystem(self):
        """Research the arts and cultural technology ecosystem"""
        print("🎭 Researching arts technology ecosystem...")
        
        # Direct competitors and adjacent companies to Spektrix, AudienceView, Tessitura
        arts_companies = [
            # Ticketing & Event Management
            {
                'company_name': 'Eventbrite',
                'description': 'Event discovery and ticketing platform',
                'industry': 'Event Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'Event ticketing, similar to AudienceView'
            },
            {
                'company_name': 'Universe (acquired by Ticketmaster)',
                'description': 'Event ticketing and promotion platform',
                'industry': 'Event Technology', 
                'focus_area': 'anne_focus',
                'why_relevant': 'Event ticketing, arts venue focus'
            },
            {
                'company_name': 'Brown Paper Tickets',
                'description': 'Event ticketing for independent venues',
                'industry': 'Event Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'Independent arts venues, similar market'
            },
            
            # Arts & Cultural Institution Management
            {
                'company_name': 'PatronManager',
                'description': 'CRM and fundraising for arts organizations',
                'industry': 'Arts Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'Direct competitor to Tessitura CRM features'
            },
            {
                'company_name': 'Blackbaud',
                'description': 'Nonprofit and arts organization software',
                'industry': 'Nonprofit Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'Serves arts organizations, fundraising focus'
            },
            {
                'company_name': 'DonorPerfect',
                'description': 'Donor management for nonprofits and arts',
                'industry': 'Nonprofit Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'Fundraising for cultural institutions'
            },
            {
                'company_name': 'Artlogic',
                'description': 'Gallery and art collection management',
                'industry': 'Arts Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'Cultural institution management'
            },
            
            # Higher Education Event/Venue Management
            {
                'company_name': 'EMS Software',
                'description': 'Event and space management for universities',
                'industry': 'Higher Education Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'University events, leverages EdTech background'
            },
            {
                'company_name': 'CollegiateLink (Campus Labs)',
                'description': 'Student engagement and event management',
                'industry': 'Higher Education Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'Campus events, EdTech background relevant'
            },
            
            # Broader Event Technology
            {
                'company_name': 'Cvent',
                'description': 'Enterprise event management platform',
                'industry': 'Event Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'Large-scale event management'
            },
            {
                'company_name': 'Bizzabo',
                'description': 'Event experience platform',
                'industry': 'Event Technology',
                'focus_area': 'anne_focus',
                'why_relevant': 'Event technology and analytics'
            }
        ]
        
        for company in arts_companies:
            company['source'] = 'Arts Tech Ecosystem Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
        
        print(f"  Found {len(arts_companies)} arts technology companies")
        return arts_companies
    
    def research_global_fintech_ecosystem(self):
        """Research global fintech companies relevant to Alessandro's background"""
        print("🌍 Researching global fintech ecosystem...")
        
        global_fintech_companies = [
            # International Money Transfer & Payments
            {
                'company_name': 'Wise (formerly TransferWise)',
                'description': 'International money transfers and multi-currency banking',
                'industry': 'Global Fintech',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Similar to Western Union, European roots, AML compliance'
            },
            {
                'company_name': 'Remitly',
                'description': 'Digital remittances to emerging markets',
                'industry': 'Global Fintech',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Similar to MoneyGram, compliance expertise needed'
            },
            {
                'company_name': 'Ria Money Transfer',
                'description': 'International money transfer services',
                'industry': 'Global Fintech',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Direct competitor to Western Union/MoneyGram'
            },
            
            # European Fintech with US Operations
            {
                'company_name': 'Klarna',
                'description': 'Swedish buy-now-pay-later, US expansion',
                'industry': 'Consumer Fintech',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'European company, US expansion, multilingual ops'
            },
            {
                'company_name': 'Revolut',
                'description': 'UK digital bank expanding globally',
                'industry': 'Digital Banking',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'European fintech, US expansion, compliance'
            },
            {
                'company_name': 'N26',
                'description': 'German digital bank with US operations',
                'industry': 'Digital Banking',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'German bank, US market entry, speaks German'
            },
            
            # Payment Infrastructure
            {
                'company_name': 'Stripe',
                'description': 'Global payment infrastructure',
                'industry': 'Payment Infrastructure',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Global payments, European operations, compliance'
            },
            {
                'company_name': 'Adyen',
                'description': 'Dutch payment platform, global operations',
                'industry': 'Payment Infrastructure',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'European payment company, global compliance'
            },
            {
                'company_name': 'Checkout.com',
                'description': 'UK payment processing platform',
                'industry': 'Payment Infrastructure',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'European payments, US expansion'
            },
            
            # Compliance & Identity
            {
                'company_name': 'Jumio',
                'description': 'Identity verification and compliance',
                'industry': 'Identity & Compliance',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'KYC/AML compliance, international regulations'
            },
            {
                'company_name': 'Onfido',
                'description': 'Identity verification for financial services',
                'industry': 'Identity & Compliance',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'KYC compliance, fintech focus'
            }
        ]
        
        for company in global_fintech_companies:
            company['source'] = 'Global Fintech Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
        
        print(f"  Found {len(global_fintech_companies)} global fintech companies")
        return global_fintech_companies
    
    def research_public_safety_ecosystem(self):
        """Research public safety and government technology companies"""
        print("🚨 Researching public safety ecosystem...")
        
        public_safety_companies = [
            # Direct competitors to Motorola
            {
                'company_name': 'Tyler Technologies',
                'description': 'Software solutions for state and local government',
                'industry': 'Government Technology',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Public safety software, government clients'
            },
            {
                'company_name': 'Axon',
                'description': 'Body cameras and digital evidence management',
                'industry': 'Public Safety Technology',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Similar to Motorola CommandCentral products'
            },
            {
                'company_name': 'Mark43',
                'description': 'Cloud-based software for law enforcement',
                'industry': 'Public Safety Technology',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Modern police software, cloud deployment'
            },
            {
                'company_name': 'Genetec',
                'description': 'Security and public safety software platforms',
                'industry': 'Security Technology',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Public safety software, international operations'
            },
            
            # Emergency Services & Communication
            {
                'company_name': 'Everbridge',
                'description': 'Critical event management platform',
                'industry': 'Emergency Management',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Emergency communications, government clients'
            },
            {
                'company_name': 'Zoll Data Systems',
                'description': 'Software for EMS and fire departments',
                'industry': 'Emergency Services Technology',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Emergency services, public safety focus'
            },
            
            # Government & Defense
            {
                'company_name': 'Palantir',
                'description': 'Data analytics for government and defense',
                'industry': 'Government Analytics',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Government contracts, defense applications'
            },
            {
                'company_name': 'Verint',
                'description': 'Intelligence and security analytics',
                'industry': 'Security Analytics',
                'focus_area': 'alessandro_focus',
                'why_relevant': 'Government and security applications'
            }
        ]
        
        for company in public_safety_companies:
            company['source'] = 'Public Safety Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
        
        print(f"  Found {len(public_safety_companies)} public safety companies")
        return public_safety_companies
    
    def validate_careers_pages(self, companies):
        """Validate and find careers pages for companies"""
        print("🔗 Validating careers pages...")
        
        for i, company in enumerate(companies):
            if i % 10 == 0:
                print(f"  Validated {i}/{len(companies)} companies...")
            
            company_name = company['company_name']
            careers_url = self.find_careers_page(company_name, company.get('company_page'))
            
            company['careers_url'] = careers_url or "Not found"
            company['careers_validated'] = careers_url is not None
            
            time.sleep(1)  # Rate limiting
        
        print(f"  Careers page validation complete")
        return companies
    
    def find_careers_page(self, company_name, company_url=None):
        """Find careers page for a company"""
        potential_urls = []
        
        # If we have a company URL, use it as base
        if company_url:
            if company_url.startswith('http'):
                base_url = company_url.rstrip('/')
            else:
                base_url = f"https://{company_url.strip('/')}"
                
            potential_urls.extend([
                f"{base_url}/careers",
                f"{base_url}/jobs",
                f"{base_url}/careers/",
                f"{base_url}/about/careers",
                f"{base_url}/company/careers"
            ])
        
        # Generate URLs from company name
        company_slug = re.sub(r'[^a-zA-Z0-9]', '', company_name.lower())
        company_slug_dash = re.sub(r'[^a-zA-Z0-9]', '-', company_name.lower()).strip('-')
        
        potential_urls.extend([
            f"https://{company_slug}.com/careers",
            f"https://www.{company_slug}.com/careers",
            f"https://{company_slug}.com/jobs",
            f"https://careers.{company_slug}.com",
            f"https://{company_slug_dash}.com/careers",
            f"https://www.{company_slug_dash}.com/careers"
        ])
        
        # Test URLs
        for url in potential_urls:
            try:
                response = self.session.get(url, timeout=5)
                if response.status_code == 200:
                    # Check if it looks like a careers page
                    content = response.text.lower()
                    career_indicators = ['job', 'career', 'position', 'hiring', 'apply', 'openings', 'opportunities']
                    if any(indicator in content for indicator in career_indicators):
                        return url
            except:
                continue
        
        return None
    
    def run_comprehensive_research(self):
        """Run comprehensive company research"""
        print("🔍 Starting comprehensive company research...")
        
        all_companies = []
        
        # Research methods
        research_methods = [
            ('Built In Colorado', lambda: self.scrape_built_in_companies('colorado')),
            ('Arts Technology', self.research_arts_tech_ecosystem),
            ('Global Fintech', self.research_global_fintech_ecosystem),
            ('Public Safety', self.research_public_safety_ecosystem)
        ]
        
        # Run each research method
        for method_name, method_func in research_methods:
            try:
                print(f"\n📊 Running {method_name} research...")
                companies = method_func()
                all_companies.extend(companies)
                print(f"✅ {method_name}: {len(companies)} companies found")
                time.sleep(3)  # Rate limiting between methods
            except Exception as e:
                print(f"❌ Error in {method_name} research: {e}")
                continue
        
        # Validate careers pages
        if all_companies:
            all_companies = self.validate_careers_pages(all_companies)
        
        return all_companies
    
    def analyze_and_categorize_companies(self, companies):
        """Analyze companies and categorize by relevance"""
        print("📈 Analyzing and categorizing companies...")
        
        # Add relevance scoring
        for company in companies:
            company['relevance_score'] = self.calculate_relevance_score(company)
            company['recommended_for'] = self.get_recommendation(company)
        
        # Sort by relevance score
        companies.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
        
        return companies
    
    def calculate_relevance_score(self, company):
        """Calculate relevance score based on company attributes"""
        score = 0
        
        description = company.get('description', '').lower()
        industry = company.get('industry', '').lower()
        company_name = company.get('company_name', '').lower()
        
        # Anne's interests (arts, events, EdTech adjacent)
        anne_keywords = [
            'event', 'ticket', 'arts', 'cultural', 'museum', 'theater', 'music',
            'nonprofit', 'education', 'university', 'student', 'learning',
            'venue', 'patron', 'audience', 'performance'
        ]
        
        # Alessandro's interests (global, fintech, public safety, European)
        alessandro_keywords = [
            'global', 'international', 'payment', 'fintech', 'money', 'transfer',
            'compliance', 'aml', 'kyc', 'european', 'german', 'italian',
            'public safety', 'emergency', 'government', 'security', 'defense'
        ]
        
        # Calculate scores
        for keyword in anne_keywords:
            if keyword in description or keyword in industry or keyword in company_name:
                score += 1
        
        for keyword in alessandro_keywords:
            if keyword in description or keyword in industry or keyword in company_name:
                score += 1
        
        # Bonus for validated careers page
        if company.get('careers_validated'):
            score += 2
        
        # Bonus for location relevance
        if company.get('location') in ['Denver', 'Colorado', 'Boulder']:
            score += 1
        
        return score
    
    def get_recommendation(self, company):
        """Get recommendation for who should focus on this company"""
        description = company.get('description', '').lower()
        industry = company.get('industry', '').lower()
        focus_area = company.get('focus_area', '')
        
        if focus_area:
            return focus_area
        
        # Determine based on keywords
        anne_indicators = ['event', 'ticket', 'arts', 'cultural', 'education', 'nonprofit']
        alessandro_indicators = ['fintech', 'payment', 'global', 'security', 'government', 'compliance']
        
        anne_score = sum(1 for keyword in anne_indicators if keyword in description or keyword in industry)
        alessandro_score = sum(1 for keyword in alessandro_indicators if keyword in description or keyword in industry)
        
        if anne_score > alessandro_score:
            return 'anne_focus'
        elif alessandro_score > anne_score:
            return 'alessandro_focus'
        else:
            return 'both'
    
    def save_research_results(self, companies):
        """Save comprehensive research results"""
        if not companies:
            print("❌ No companies found to save")
            return
        
        print("💾 Saving research results...")
        
        # Create research directory
        os.makedirs('research_results', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create DataFrame
        df = pd.DataFrame(companies)
        
        # Save detailed research results
        detailed_file = f'research_results/detailed_company_research_{timestamp}.csv'
        df.to_csv(detailed_file, index=False)
        
        # Create integration-ready format
        integration_df = df.copy()
        integration_df['Company'] = integration_df['company_name']
        integration_df['Careers Site URL'] = integration_df['careers_url']
        integration_df['Industry'] = integration_df['industry']
        integration_df['Primary_Source'] = 'careers_page'
        integration_df['Company_Size'] = 'Medium'  # Default
        integration_df['City'] = integration_df.get('location', 'Denver')
        
        # Filter companies with valid careers pages
        valid_companies = integration_df[integration_df['careers_validated'] == True]
        
        # Save integration file
        integration_columns = ['Company', 'Careers Site URL', 'Industry', 'Primary_Source', 'Company_Size', 'City']
        integration_file = f'research_results/companies_to_integrate_{timestamp}.csv'
        valid_companies[integration_columns].to_csv(integration_file, index=False)
        
        # Create prioritized recommendations
        anne_companies = df[df['recommended_for'].isin(['anne_focus', 'both'])].head(20)
        alessandro_companies = df[df['recommended_for'].isin(['alessandro_focus', 'both'])].head(20)
        
        anne_file = f'research_results/anne_recommended_companies_{timestamp}.csv'
        alessandro_file = f'research_results/alessandro_recommended_companies_{timestamp}.csv'
        
        anne_companies.to_csv(anne_file, index=False)
        alessandro_companies.to_csv(alessandro_file, index=False)
        
        # Create comprehensive summary
        summary = {
            'research_date': datetime.now().isoformat(),
            'total_companies_researched': len(companies),
            'companies_with_careers_pages': len(valid_companies),
            'anne_focused_companies': len(anne_companies),
            'alessandro_focused_companies': len(alessandro_companies),
            'top_anne_companies': [
                {
                    'name': row['company_name'],
                    'industry': row['industry'],
                    'score': row['relevance_score'],
                    'why_relevant': row.get('why_relevant', 'Industry match')
                }
                for _, row in anne_companies.head(5).iterrows()
            ],
            'top_alessandro_companies': [
                {
                    'name': row['company_name'],
                    'industry': row['industry'], 
                    'score': row['relevance_score'],
                    'why_relevant': row.get('why_relevant', 'Industry match')
                }
                for _, row in alessandro_companies.head(5).iterrows()
            ],
            'research_sources': list(set(df['source'].tolist())),
            'files_created': {
                'detailed_research': detailed_file,
                'integration_ready': integration_file,
                'anne_recommendations': anne_file,
                'alessandro_recommendations': alessandro_file
            }
        }
        
        summary_file = f'research_results/research_summary_{timestamp}.json'
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Print results
        print(f"\n🎉 RESEARCH COMPLETE!")
        print(f"📊 Total companies researched: {summary['total_companies_researched']}")
        print(f"✅ Companies with careers pages: {summary['companies_with_careers_pages']}")
        print(f"🎭 Anne-focused companies: {summary['anne_focused_companies']}")
        print(f"🌍 Alessandro-focused companies: {summary['alessandro_focused_companies']}")
        
        print(f"\n🏆 Top recommendations for Anne:")
        for company in summary['top_anne_companies']:
            print(f"  • {company['name']} ({company['industry']}) - Score: {company['score']}")
        
        print(f"\n🏆 Top recommendations for Alessandro:")
        for company in summary['top_alessandro_companies']:
            print(f"  • {company['name']} ({company['industry']}) - Score: {company['score']}")
        
        print(f"\n📁 Files created:")
        for file_type, filename in summary['files_created'].items():
            print(f"  • {file_type}: {filename}")
        
        return summary

if __name__ == "__main__":
    researcher = AdvancedCompanyResearcher()
    companies = researcher.run_comprehensive_research()
    
    if companies:
        companies = researcher.analyze_and_categorize_companies(companies)
        summary = researcher.save_research_results(companies)
    else:
        print("❌ No companies found during research")
