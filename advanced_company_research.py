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
                if response
