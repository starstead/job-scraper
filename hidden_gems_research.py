import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime
import os

class HiddenGemsResearcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    
    def research_startup_databases(self):
        """Research smaller startups and growing companies"""
        print("🚀 Researching hidden startup gems...")
        
        # Lesser-known but growing companies in your target areas
        hidden_gems = [
            # Arts/Cultural Tech (smaller players)
            {
                'company_name': 'TicketSource',
                'description': 'Ticketing platform for independent venues and arts organizations',
                'industry': 'Arts Technology',
                'company_size': 'Small',
                'why_hidden': 'UK-based, smaller than Eventbrite but growing',
                'focus': 'anne'
            },
            {
                'company_name': 'ShowClix',
                'description': 'Ticketing and marketing for mid-size venues',
                'industry': 'Event Technology',
                'company_size': 'Small',
                'why_hidden': 'Pittsburgh-based, focuses on mid-tier venues',
                'focus': 'anne'
            },
            {
                'company_name': 'TryBooking',
                'description': 'Event ticketing platform from Australia expanding globally',
                'industry': 'Event Technology',
                'company_size': 'Medium',
                'why_hidden': 'Australian company, less known in US market',
                'focus': 'anne'
            },
            {
                'company_name': 'Artsy',
                'description': 'Online platform for discovering and collecting art',
                'industry': 'Arts Technology',
                'company_size': 'Medium',
                'why_hidden': 'Niche market, art world focus',
                'focus': 'anne'
            },
            {
                'company_name': 'Givebutter',
                'description': 'Fundraising platform for nonprofits and events',
                'industry': 'Nonprofit Technology',
                'company_size': 'Small',
                'why_hidden': 'Growing but still under Fortune 500 radar',
                'focus': 'anne'
            },
            {
                'company_name': 'RunSignup',
                'description': 'Race registration and event management for running events',
                'industry': 'Event Technology',
                'company_size': 'Small',
                'why_hidden': 'Niche focus on race events, family-owned',
                'focus': 'anne'
            },
            
            # Fintech Hidden Gems (smaller/emerging)
            {
                'company_name': 'Currencycloud',
                'description': 'B2B cross-border payment infrastructure',
                'industry': 'Fintech Infrastructure',
                'company_size': 'Medium',
                'why_hidden': 'B2B focus, less consumer-facing than Wise',
                'focus': 'alessandro'
            },
            {
                'company_name': 'Thunes',
                'description': 'Cross-border payment network for emerging markets',
                'industry': 'Global Fintech',
                'company_size': 'Medium',
                'why_hidden': 'Singapore-based, B2B focus, emerging markets',
                'focus': 'alessandro'
            },
            {
                'company_name': 'Zepz (WorldRemit)',
                'description': 'Digital money transfer to emerging markets',
                'industry': 'Global Fintech',
                'company_size': 'Medium',
                'why_hidden': 'UK-based, focus on underserved markets',
                'focus': 'alessandro'
            },
            {
                'company_name': 'Nium',
                'description': 'Global payments infrastructure for businesses',
                'industry': 'Fintech Infrastructure',
                'company_size': 'Medium',
                'why_hidden': 'Singapore-based, B2B infrastructure play',
                'focus': 'alessandro'
            },
            {
                'company_name': 'Flywire',
                'description': 'Cross-border payments for education and healthcare',
                'industry': 'Vertical Fintech',
                'company_size': 'Medium',
                'why_hidden': 'Vertical focus, education payments niche',
                'focus': 'alessandro'
            },
            {
                'company_name': 'Tribal Credit',
                'description': 'Corporate cards for emerging markets',
                'industry': 'Corporate Fintech',
                'company_size': 'Small',
                'why_hidden': 'Latin America focus, smaller market',
                'focus': 'alessandro'
            },
            
            # Public Safety Hidden Gems
            {
                'company_name': 'CivicEye',
                'description': 'Community policing and transparency platform',
                'industry': 'Public Safety Technology',
                'company_size': 'Small',
                'why_hidden': 'Newer player, community-focused approach',
                'focus': 'alessandro'
            },
            {
                'company_name': 'CitizenAid',
                'description': 'Emergency response and first aid guidance app',
                'industry': 'Emergency Technology',
                'company_size': 'Small',
                'why_hidden': 'UK startup, niche emergency response',
                'focus': 'alessandro'
            },
            
            # Denver/Colorado Hidden Gems
            {
                'company_name': 'Snyk',
                'description': 'Security platform for developers (Denver office)',
                'industry': 'Developer Security',
                'company_size': 'Medium',
                'why_hidden': 'Developer-focused, growing but not Fortune 500',
                'focus': 'both'
            },
            {
                'company_name': 'Gitlab',
                'description': 'DevOps platform (remote-first, some Denver talent)',
                'industry': 'Developer Tools',
                'company_size': 'Medium',
                'why_hidden': 'Remote-first, strong product culture',
                'focus': 'both'
            },
            {
                'company_name': 'Sphere',
                'description': 'Identity verification for sharing economy',
                'industry': 'Identity Technology',
                'company_size': 'Small',
                'why_hidden': 'Denver-based, niche market, growing',
                'focus': 'both'
            },
            {
                'company_name': 'Revolar',
                'description': 'Personal safety devices and platform',
                'industry': 'Safety Technology',
                'company_size': 'Small',
                'why_hidden': 'Denver-based, personal safety niche',
                'focus': 'both'
            },
            {
                'company_name': 'LogRhythm',
                'description': 'Security information and event management',
                'industry': 'Security Technology',
                'company_size': 'Medium',
                'why_hidden': 'Boulder-based, enterprise security focus',
                'focus': 'alessandro'
            },
            
            # European Companies with US Expansion (Great for Alessandro)
            {
                'company_name': 'SumUp',
                'description': 'European payment solutions expanding globally',
                'industry': 'Payment Technology',
                'company_size': 'Medium',
                'why_hidden': 'German company, US expansion opportunity',
                'focus': 'alessandro'
            },
            {
                'company_name': 'Mollie',
                'description': 'Dutch payment processor expanding internationally',
                'industry': 'Payment Processing',
                'company_size': 'Medium',
                'why_hidden': 'Netherlands-based, growing in US',
                'focus': 'alessandro'
            },
            {
                'company_name': 'Contis',
                'description': 'UK banking-as-a-service platform',
                'industry': 'Banking Infrastructure',
                'company_size': 'Small',
                'why_hidden': 'UK fintech, B2B focus, under the radar',
                'focus': 'alessandro'
            },
            
            # EdTech Adjacent (Good for Anne)
            {
                'company_name': 'Whova',
                'description': 'Event networking and engagement platform',
                'industry': 'Event Technology',
                'company_size': 'Small',
                'why_hidden': 'Conference tech, education market overlap',
                'focus': 'anne'
            },
            {
                'company_name': 'Goldcast',
                'description': 'B2B event marketing platform',
                'industry': 'Event Marketing Technology',
                'company_size': 'Small',
                'why_hidden': 'Newer player, B2B event focus',
                'focus': 'anne'
            },
            {
                'company_name': 'Pathable',
                'description': 'Community and event engagement platform',
                'industry': 'Community Technology',
                'company_size': 'Small',
                'why_hidden': 'Niche community building for events',
                'focus': 'anne'
            }
        ]
        
        for company in hidden_gems:
            company['source'] = 'Hidden Gems Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
            company['location'] = 'Various'
        
        print(f"  Found {len(hidden_gems)} hidden gem companies")
        return hidden_gems
    
    def research_recent_funding(self):
        """Research recently funded smaller companies"""
        print("💰 Researching recently funded smaller companies...")
        
        # Companies that recently raised Series A/B (good hiring phase)
        funded_gems = [
            {
                'company_name': 'Ramp',
                'description': 'Corporate expense management and cards',
                'industry': 'Corporate Fintech',
                'company_size': 'Medium',
                'funding_stage': 'Series C',
                'why_hidden': 'B2B focus, growing fast but still hiring aggressively',
                'focus': 'alessandro'
            },
            {
                'company_name': 'Mercury',
                'description': 'Banking for startups and small businesses',
                'industry': 'Business Banking',
                'company_size': 'Small',
                'funding_stage': 'Series B',
                'why_hidden': 'Startup-focused, smaller than traditional banks',
                'focus': 'alessandro'
            },
            {
                'company_name': 'Airtable',
                'description': 'Low-code platform for building collaborative apps',
                'industry': 'Productivity Software',
                'company_size': 'Medium',
                'funding_stage': 'Series E',
                'why_hidden': 'Product-focused culture, strong PM opportunities',
                'focus': 'both'
            },
            {
                'company_name': 'Figma',
                'description': 'Collaborative design platform',
                'industry': 'Design Technology',
                'company_size': 'Medium',
                'funding_stage': 'Acquired by Adobe',
                'why_hidden': 'Design-focused, product-driven culture',
                'focus': 'anne'
            },
            {
                'company_name': 'Linear',
                'description': 'Issue tracking and project management for software teams',
                'industry': 'Developer Tools',
                'company_size': 'Small',
                'funding_stage': 'Series A',
                'why_hidden': 'Developer-focused, high-quality product culture',
                'focus': 'both'
            }
        ]
        
        for company in funded_gems:
            company['source'] = 'Recent Funding Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
        
        print(f"  Found {len(funded_gems)} recently funded companies")
        return funded_gems
    
    def research_b2b_saas_gems(self):
        """Research B2B SaaS companies that are under the radar"""
        print("💼 Researching B2B SaaS hidden gems...")
        
        b2b_gems = [
            {
                'company_name': 'Retool',
                'description': 'Low-code platform for building internal tools',
                'industry': 'Developer Tools',
                'company_size': 'Medium',
                'why_hidden': 'Developer-focused, internal tools niche',
                'focus': 'both'
            },
            {
                'company_name': 'PostHog',
                'description': 'Open-source product analytics platform',
                'industry': 'Analytics',
                'company_size': 'Small',
                'why_hidden': 'Open-source, developer-focused analytics',
                'focus': 'anne'
            },
            {
                'company_name': 'Amplitude',
                'description': 'Product analytics for digital products',
                'industry': 'Product Analytics',
                'company_size': 'Medium',
                'why_hidden': 'Product analytics niche, strong PM culture',
                'focus': 'anne'
            },
            {
                'company_name': 'LaunchDarkly',
                'description': 'Feature flag and experimentation platform',
                'industry': 'Developer Tools',
                'company_size': 'Medium',
                'why_hidden': 'Developer tooling, experimentation focus',
                'focus': 'both'
            },
            {
                'company_name': 'PlanetScale',
                'description': 'Database platform for modern applications',
                'industry': 'Database Technology',
                'company_size': 'Small',
                'why_hidden': 'Technical product, database infrastructure',
                'focus': 'both'
            }
        ]
        
        for company in b2b_gems:
            company['source'] = 'B2B SaaS Research'
            company['research_date'] = datetime.now().strftime('%Y-%m-%d')
        
        print(f"  Found {len(b2b_gems)} B2B SaaS gems")
        return b2b_gems
    
    def validate_careers_pages_batch(self, companies):
        """Validate careers pages for all companies"""
        print("🔗 Validating careers pages...")
        
        for i, company in enumerate(companies):
            if i % 5 == 0:
                print(f"  Validated {i}/{len(companies)} companies...")
            
            careers_url = self.find_careers_page(company['company_name'])
            company['careers_url'] = careers_url or "Not found"
            company['careers_validated'] = careers_url is not None
            
            time.sleep(2)  # Rate limiting
        
        validated_count = sum(1 for c in companies if c['careers_validated'])
        print(f"  ✅ Found careers pages for {validated_count}/{len(companies)} companies")
        
        return companies
    
    def find_careers_page(self, company_name):
        """Find careers page for a company"""
        # Generate potential URLs
        company_slug = re.sub(r'[^a-zA-Z0-9]', '', company_name.lower())
        company_slug_dash = re.sub(r'[^a-zA-Z0-9]', '-', company_name.lower()).strip('-')
        
        potential_urls = [
            f"https://{company_slug}.com/careers",
            f"https://www.{company_slug}.com/careers",
            f"https://{company_slug}.com/jobs",
            f"https://careers.{company_slug}.com",
            f"https://{company_slug_dash}.com/careers",
            f"https://www.{company_slug_dash}.com/careers",
            f"https://{company_slug}.com/about/careers",
            f"https://{company_slug}.io/careers",
            f"https://{company_slug}.co/careers"
        ]
        
        for url in potential_urls:
            try:
                response = self.session.get(url, timeout=5)
                if response.status_code == 200:
                    content = response.text.lower()
                    if any(word in content for word in ['job', 'career', 'position', 'hiring', 'openings']):
                        return url
            except:
                continue
        
        return None
    
    def run_hidden_gems_research(self):
        """Run comprehensive hidden gems research"""
        print("🔍 Starting Hidden Gems Company Research...")
        print("🎯 Focusing on smaller, growing companies others might miss")
        
        all_companies = []
        
        # Research methods
        research_methods = [
            ('Startup Hidden Gems', self.research_startup_databases),
            ('Recently Funded', self.research_recent_funding),
            ('B2B SaaS Gems', self.research_b2b_saas_gems)
        ]
        
        # Run research
        for method_name, method_func in research_methods:
            try:
                print(f"\n📊 {method_name}...")
                companies = method_func()
                all_companies.extend(companies)
                time.sleep(2)
            except Exception as e:
                print(f"❌ Error in {method_name}: {e}")
        
        # Validate careers pages
        if all_companies:
            all_companies = self.validate_careers_pages_batch(all_companies)
        
        return all_companies
    
    def save_hidden_gems_results(self, companies):
        """Save hidden gems research results"""
        if not companies:
            print("❌ No companies found")
            return
        
        print("💾 Saving hidden gems research...")
        
        os.makedirs('research_results', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create DataFrame
        df = pd.DataFrame(companies)
        
        # Add scoring for prioritization
        for i, company in enumerate(companies):
            score = 0
            # Small companies get higher scores
            if company.get('company_size') == 'Small':
                score += 3
            elif company.get('company_size') == 'Medium':
                score += 2
            
            # Valid careers page
            if company.get('careers_validated'):
                score += 2
            
            # Recent funding
            if 'funding_stage' in company:
                score += 1
            
            companies[i]['priority_score'] = score
        
        # Sort by priority score
        df = pd.DataFrame(companies)
        df = df.sort_values('priority_score', ascending=False)
        
        # Save detailed results
        detailed_file = f'research_results/hidden_gems_detailed_{timestamp}.csv'
        df.to_csv(detailed_file, index=False)
        
        # Create integration format
        df['Company'] = df['company_name']
        df['Careers Site URL'] = df['careers_url']
        df['Industry'] = df['industry']
        df['Primary_Source'] = 'careers_page'
        df['Company_Size'] = df['company_size']
        df['City'] = 'Various'
        
        # Filter valid companies
        valid_companies = df[df['careers_validated'] == True]
        integration_columns = ['Company', 'Careers Site URL', 'Industry', 'Primary_Source', 'Company_Size', 'City']
        integration_file = f'research_results/hidden_gems_integration_{timestamp}.csv'
        valid_companies[integration_columns].to_csv(integration_file, index=False)
        
        # Create focus-specific files
        anne_companies = df[df['focus'].isin(['anne', 'both'])].head(15)
        alessandro_companies = df[df['focus'].isin(['alessandro', 'both'])].head(15)
        
        anne_file = f'research_results/anne_hidden_gems_{timestamp}.csv'
        alessandro_file = f'research_results/alessandro_hidden_gems_{timestamp}.csv'
        
        anne_companies.to_csv(anne_file, index=False)
        alessandro_companies.to_csv(alessandro_file, index=False)
        
        # Summary
        summary = {
            'research_date': datetime.now().isoformat(),
            'total_companies': len(companies),
            'companies_with_careers_pages': len(valid_companies),
            'anne_focused': len(anne_companies),
            'alessandro_focused': len(alessandro_companies),
            'small_companies': len(df[df['company_size'] == 'Small']),
            'medium_companies': len(df[df['company_size'] == 'Medium']),
            'top_hidden_gems': [
                {
                    'name': row['company_name'],
                    'industry': row['industry'],
                    'size': row['company_size'],
                    'why_hidden': row.get('why_hidden', 'Growing company'),
                    'score': row['priority_score']
                }
                for _, row in df.head(10).iterrows()
            ]
        }
        
        summary_file = f'research_results/hidden_gems_summary_{timestamp}.json'
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n🎉 HIDDEN GEMS RESEARCH COMPLETE!")
        print(f"💎 Total hidden gems: {summary['total_companies']}")
        print(f"✅ With careers pages: {summary['companies_with_careers_pages']}")
        print(f"🏢 Small companies: {summary['small_companies']}")
        print(f"🏢 Medium companies: {summary['medium_companies']}")
        
        print(f"\n🏆 Top Hidden Gems:")
        for gem in summary['top_hidden_gems'][:5]:
            print(f"  • {gem['name']} ({gem['size']}) - {gem['industry']}")
            print(f"    Why hidden: {gem['why_hidden']}")
        
        print(f"\n📁 Files created:")
        print(f"  • Detailed: {detailed_file}")
        print(f"  • Integration: {integration_file}")
        print(f"  • Anne's gems: {anne_file}")
        print(f"  • Alessandro's gems: {alessandro_file}")
        
        return summary

if __name__ == "__main__":
    researcher = HiddenGemsResearcher()
    companies = researcher.run_hidden_gems_research()
    if companies:
        researcher.save_hidden_gems_results(companies)
