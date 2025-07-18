import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime
import os
import hashlib
from notion_client import Client

class MultiPlatformJobScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # Rate limiting per platform
        self.rate_limits = {
            'careers': 2,     # 2 seconds between careers page requests
            'indeed': 4,      # 4 seconds between Indeed requests
            'angellist': 3,   # 3 seconds between AngelList requests
            'glassdoor': 5    # 5 seconds between Glassdoor requests
        }
    
    def load_companies(self):
        """Load the final ready companies file"""
        try:
            df = pd.read_csv('companies_final_ready.csv')
            print(f"✅ Loaded {len(df)} companies from companies_final_ready.csv")
            return df.to_dict('records')
        except FileNotFoundError:
            print("❌ companies_final_ready.csv not found!")
            return []
    
    def load_keywords(self):
        """Load keywords from config"""
        try:
            with open('config_fixed.json', 'r') as f:
                config = json.load(f)
            keywords = config.get('keywords', [])
            print(f"✅ Loaded {len(keywords)} keywords from config")
            return keywords
        except:
            # Default keywords if no config file
            return [
                "product manager", "senior product manager", "associate product manager",
                "principal product manager", "group product manager", "product owner", 
                "head of product", "director of product management", "project manager",
                "senior project manager", "program manager", "senior program manager",
                "technical project manager", "technical program manager", "portfolio manager",
                "implementation manager", "implementation project manager", "PMO manager",
                "manager of program management", "manager of project management",
                "director of program management", "director of project management", 
                "strategy manager", "business analyst", "integration product manager",
                "product manager mergers and acquisitions", "M&A product manager",
                "post-merger integration manager", "integration program manager",
                "customer success operations manager", "customer success strategy manager",
                "product operations manager", "customer experience manager",
                "product-led customer success manager", "remote", "hybrid"
            ]
    
    def scrape_careers_page(self, url, keywords, company_name):
        """Scrape company careers page (primary method)"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Better job element detection - find individual job cards
            job_elements = []
            
            # Strategy 1: Look for job cards with specific patterns
            job_card_selectors = [
                ['div', {'class': re.compile(r'job.*card|position.*card|career.*card', re.I)}],
                ['article', {'class': re.compile(r'job|position|career', re.I)}],
                ['section', {'class': re.compile(r'job|position|career', re.I)}],
                ['li', {'class': re.compile(r'job|position|career', re.I)}],
                ['div', {'class': re.compile(r'opening|listing|posting', re.I)}],
            ]
            
            for tag, attrs in job_card_selectors:
                elements = soup.find_all(tag, attrs)
                if elements:
                    job_elements.extend(elements)
                    print(f"Found {len(elements)} job elements using {tag}")
            
            # Strategy 2: If no job cards found, look for headings and build job elements around them
            if not job_elements:
                print("No job cards found, trying heading-based detection...")
                job_headings = soup.find_all(['h1', 'h2', 'h3', 'h4'], 
                                           string=re.compile(r'manager|analyst|director|specialist|engineer', re.I))
                
                for heading in job_headings:
                    for parent_level in [1, 2, 3]:
                        container = heading
                        for _ in range(parent_level):
                            container = container.find_parent()
                            if not container:
                                break
                        
                        if container:
                            container_text = container.get_text().strip()
                            if 50 < len(container_text) < 2000:
                                job_elements.append(container)
                                break
            
            # Strategy 3: Fallback to whole page
            if not job_elements:
                job_elements = [soup]
                print("Using whole page as single element (fallback)")
            
            # Remove duplicates
            unique_elements = []
            for element in job_elements:
                if element not in unique_elements:
                    unique_elements.append(element)
            
            job_elements = unique_elements[:5]  # Limit to 5 elements for speed
            print(f"Processing {len(job_elements)} job elements")
            
            jobs_found = []
            
            for element in job_elements:
                try:
                    element_text = element.get_text().lower()
                    
                    # Check for keyword matches
                    found_keywords = []
                    for keyword in keywords:
                        if keyword.lower() in element_text:
                            found_keywords.append(keyword)
                    
                    # Separate core job keywords from modifiers  
                    core_keywords = [kw for kw in found_keywords if kw.lower() not in ['remote', 'hybrid']]
                    modifier_keywords = [kw for kw in found_keywords if kw.lower() in ['remote', 'hybrid']]
                    
                    # Only process if we have core job keywords
                    if core_keywords:
                        # Enhanced job title extraction
                        title = "Job Opening"
                        title_found = False
                        
                        # Strategy 1: Look for exact keyword matches in headings
                        for keyword in core_keywords:
                            if title_found:
                                break
                            keyword_headings = element.find_all(['h1', 'h2', 'h3', 'h4', 'h5'])
                            for heading in keyword_headings:
                                heading_text = heading.get_text().strip()
                                if keyword.lower() in heading_text.lower() and len(heading_text) > 5:
                                    if any(job_word in heading_text.lower() for job_word in ['manager', 'analyst', 'director', 'specialist']):
                                        title = heading_text
                                        title_found = True
                                        break
                        
                        # Strategy 2: Look for job-specific CSS classes
                        if not title_found:
                            title_elements = element.find_all(['h1', 'h2', 'h3', 'h4'], 
                                                            class_=re.compile(r'job.*title|title.*job|position.*title', re.I))
                            if title_elements:
                                title = title_elements[0].get_text().strip()
                                if len(title) > 5:
                                    title_found = True
                        
                        # Strategy 3: Look for strong/bold text with keywords
                        if not title_found:
                            strong_elements = element.find_all(['strong', 'b', 'h1', 'h2', 'h3', 'h4'])
                            for strong_elem in strong_elements:
                                strong_text = strong_elem.get_text().strip()
                                if any(keyword.lower() in strong_text.lower() for keyword in core_keywords):
                                    if 5 < len(strong_text) < 100:
                                        title = strong_text
                                        title_found = True
                                        break
                        
                        # Strategy 4: Fallback to first reasonable heading
                        if not title_found:
                            all_headings = element.find_all(['h1', 'h2', 'h3', 'h4'])
                            for heading in all_headings:
                                heading_text = heading.get_text().strip()
                                if 5 < len(heading_text) < 100:
                                    title = heading_text
                                    break
                        
                        # Clean up title
                        title = re.sub(r'\s+', ' ', title.replace('\n', ' ').replace('\t', ' '))
                        
                        # Extract job URL
                        job_url = url
                        link_element = element.find('a', href=True)
                        if link_element:
                            href = link_element['href']
                            if href.startswith('/'):
                                base_url = '/'.join(url.split('/')[:3])
                                job_url = base_url + href
                            elif href.startswith('http'):
                                job_url = href
                        
                        # Skip if title is too generic
                        if len(title.strip()) < 5 or title.strip().lower() in ['job opening', 'learn more', 'apply']:
                            continue
                        
                        jobs_found.append({
                            'title': title[:100],
                            'url': job_url,
                            'keywords_found': core_keywords + modifier_keywords,
                            'source': 'Careers Page'
                        })
                
                except Exception as e:
                    print(f"Error processing element: {e}")
                    continue
            
            return jobs_found
            
        except Exception as e:
            print(f"  Error scraping careers page: {str(e)[:50]}")
            return []
    
    def scrape_indeed_company(self, company_name, location="Denver,CO", keywords=None):
        """Scrape Indeed for specific company jobs"""
        try:
            query = f'company:"{company_name}"'
            url = f"https://www.indeed.com/jobs?q={query.replace(' ', '+')}&l={location}"
            
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            job_cards = soup.find_all(['div'], attrs={'data-jk': True})
            if not job_cards:
                job_cards = soup.find_all(['div'], class_=re.compile(r'job.*card|listing', re.I))
            
            jobs_found = []
            
            for card in job_cards[:6]:
                try:
                    card_text = card.get_text().lower()
                    if company_name.lower() not in card_text:
                        continue
                    
                    title_elem = card.find(['h2', 'a'], attrs={'data-testid': 'job-title'})
                    if not title_elem:
                        title_elem = card.find(['h2', 'a'])
                    
                    title = title_elem.get_text().strip() if title_elem else "Indeed Job"
                    
                    link_elem = card.find('a', href=True)
                    if link_elem and '/viewjob' in link_elem['href']:
                        job_url = "https://www.indeed.com" + link_elem['href']
                    else:
                        job_url = url
                    
                    found_keywords = []
                    if keywords:
                        for keyword in keywords:
                            if keyword.lower() in card_text:
                                found_keywords.append(keyword)
                    
                    core_keywords = [kw for kw in found_keywords if kw.lower() not in ['remote', 'hybrid']]
                    modifier_keywords = [kw for kw in found_keywords if kw.lower() in ['remote', 'hybrid']]
                    
                    if core_keywords:
                        jobs_found.append({
                            'title': title[:100],
                            'url': job_url,
                            'keywords_found': core_keywords + modifier_keywords,
                            'source': 'Indeed'
                        })
                
                except Exception as e:
                    continue
            
            return jobs_found
            
        except Exception as e:
            print(f"  Error scraping Indeed: {str(e)[:50]}")
            return []
    
    def scan_company_multiplatform(self, company_data, keywords):
        """Scan a single company across multiple platforms"""
        company_name = company_data['Company']
        primary_source = company_data.get('Primary_Source', 'careers_page')
        careers_url = company_data.get('Careers Site URL', '')
        company_size = company_data.get('Company_Size', 'Medium')
        city = company_data.get('City', 'Denver')
        
        print(f"\n📊 Scanning {company_name} ({company_size})...")
        
        all_jobs = []
        platform_stats = {'careers': 0, 'indeed': 0, 'angellist': 0, 'glassdoor': 0}
        
        # 1. Try careers page first (if available)
        if primary_source == 'careers_page' and careers_url:
            careers_jobs = self.scrape_careers_page(careers_url, keywords, company_name)
            all_jobs.extend(careers_jobs)
            platform_stats['careers'] = len(careers_jobs)
            print(f"  Careers page: {len(careers_jobs)} jobs")
            time.sleep(self.rate_limits['careers'])
        
        # 2. Use Indeed as backup if needed
        if primary_source == 'job_boards' or (len(all_jobs) == 0 and not careers_url):
            indeed_jobs = self.scrape_indeed_company(company_name, f"{city},CO", keywords)
            all_jobs.extend(indeed_jobs)
            platform_stats['indeed'] = len(indeed_jobs)
            print(f"  Indeed: {len(indeed_jobs)} jobs")
            time.sleep(self.rate_limits['indeed'])
        
        # Add company metadata to all jobs
        for job in all_jobs:
            job['company'] = company_name
            job['industry'] = company_data.get('Industry', 'Unknown')
            job['company_size'] = company_size
            job['scan_date'] = datetime.now().strftime('%Y-%m-%d')
            job['unique_id'] = self.create_unique_id(company_name, job['title'], job['url'])
        
        print(f"  Total: {len(all_jobs)} jobs")
        
        return all_jobs, platform_stats
    
    def create_unique_id(self, company, title, url):
        """Create unique ID for duplicate detection"""
        unique_string = f"{company.lower()}_{title.lower()}_{url}"
        return hashlib.md5(unique_string.encode()).hexdigest()[:16]
    
    def get_existing_jobs(self, notion, database_id):
        """Get existing jobs from Notion to check for duplicates"""
        try:
            existing_jobs = set()
            has_more = True
            start_cursor = None
            
            while has_more:
                response = notion.databases.query(
                    database_id=database_id,
                    start_cursor=start_cursor,
                    page_size=100
                )
                
                for page in response['results']:
                    try:
                        unique_id = page['properties'].get('Unique ID', {}).get('rich_text', [])
                        if unique_id:
                            existing_jobs.add(unique_id[0]['plain_text'])
                    except:
                        continue
                
                has_more = response['has_more']
                start_cursor = response.get('next_cursor')
            
            return existing_jobs
            
        except Exception as e:
            print(f"Error getting existing jobs: {e}")
            return set()
    
    def add_job_to_notion(self, notion, database_id, job_data):
        """Add a single job to Notion database"""
        try:
            new_page = {
                "parent": {"database_id": database_id},
                "properties": {
                    "Job Title": {"title": [{"text": {"content": job_data['title']}}]},
                    "Company": {"rich_text": [{"text": {"content": job_data['company']}}]},
                    "Industry": {"select": {"name": job_data['industry']}},
                    "Job URL": {"url": job_data['url']},
                    "Source": {"select": {"name": job_data['source']}},
                    "Keywords Found": {"multi_select": [{"name": kw} for kw in job_data['keywords_found']]},
                    "Date Found": {"date": {"start": datetime.now().strftime('%Y-%m-%d')}},
                    "Status": {"select": {"name": "New"}},
                    "Unique ID": {"rich_text": [{"text": {"content": job_data['unique_id']}}]}
                }
            }
            
            notion.pages.create(**new_page)
            return True
            
        except Exception as e:
            print(f"Error adding job to Notion: {e}")
            return False
    
    def run_multiplatform_scan(self):
        """Run the complete multi-platform scan"""
        # Load companies and keywords
        companies = self.load_companies()
        keywords = self.load_keywords()
        
        if not companies:
            print("❌ No companies loaded! Exiting.")
            return
        
        print(f"🚀 Starting multi-platform scan of {len(companies)} companies...")
        print(f"Keywords: {', '.join(keywords[:10])}...")
        
        # Initialize Notion if configured
        notion_token = os.getenv('NOTION_TOKEN')
        notion_database_id = os.getenv('NOTION_DATABASE_ID')
        notion = None
        existing_job_ids = set()
        
        if notion_token and notion_database_id:
            notion = Client(auth=notion_token)
            existing_job_ids = self.get_existing_jobs(notion, notion_database_id)
            print(f"Found {len(existing_job_ids)} existing jobs in Notion")
        else:
            print("Notion credentials not provided, skipping Notion sync")
        
        all_jobs = []
        total_stats = {'careers': 0, 'indeed': 0, 'angellist': 0, 'glassdoor': 0}
        companies_scanned = 0
        new_jobs_added = 0
        
        # Full scan of all companies - tested and working!
        companies_to_scan = companies
        
        for company_data in companies_to_scan:
            try:
                company_jobs, platform_stats = self.scan_company_multiplatform(
                    company_data, keywords
                )
                
                # Process jobs for Notion (if configured)
                for job in company_jobs:
                    all_jobs.append(job)
                    
                    # Add to Notion if not duplicate
                    if notion and job['unique_id'] not in existing_job_ids:
                        if self.add_job_to_notion(notion, notion_database_id, job):
                            new_jobs_added += 1
                            existing_job_ids.add(job['unique_id'])
                
                companies_scanned += 1
                
                # Update total stats
                for platform, count in platform_stats.items():
                    total_stats[platform] += count
                
                # Adaptive delay - faster for large scans
                delay = 2 if company_data.get('Company_Size') == 'Large' else 3
                time.sleep(delay)
                
            except Exception as e:
                print(f"  Error processing {company_data['Company']}: {e}")
                continue
        
        # Save results
        self.save_results(all_jobs, total_stats, companies_scanned, new_jobs_added)
        
        return all_jobs
    
    def save_results(self, all_jobs, stats, companies_scanned, new_jobs_added):
        """Save multi-platform scan results"""
        os.makedirs('results', exist_ok=True)
        
        results = {
            'scan_date': datetime.now().isoformat(),
            'companies_scanned': companies_scanned,
            'total_jobs': len(all_jobs),
            'new_jobs_added_to_notion': new_jobs_added,
            'platform_breakdown': stats,
            'jobs': all_jobs
        }
        
        with open('results/multiplatform_scan.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n🎉 MULTI-PLATFORM SCAN COMPLETE!")
        print(f"Companies scanned: {companies_scanned}")
        print(f"Total jobs found: {len(all_jobs)}")
        print(f"New jobs added to Notion: {new_jobs_added}")
        print(f"Platform breakdown:")
        for platform, count in stats.items():
            if count > 0:
                print(f"  {platform.title()}: {count} jobs")

if __name__ == "__main__":
    scraper = MultiPlatformJobScraper()
    scraper.run_multiplatform_scan()
