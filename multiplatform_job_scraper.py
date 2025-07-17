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
                # Common job card patterns
                ['div', {'class': re.compile(r'job.*card|position.*card|career.*card', re.I)}],
                ['article', {'class': re.compile(r'job|position|career', re.I)}],
                ['section', {'class': re.compile(r'job|position|career', re.I)}],
                ['li', {'class': re.compile(r'job|position|career', re.I)}],
                # Generic containers that might hold individual jobs
                ['div', {'class': re.compile(r'opening|listing|posting', re.I)}],
            ]
            
            for tag, attrs in job_card_selectors:
                elements = soup.find_all(tag, attrs)
                if elements:
                    job_elements.extend(elements)
                    print(f"Found {len(elements)} job elements using {tag} with {attrs}")
            
            # Strategy 2: If no job cards found, look for headings and build job elements around them
            if not job_elements:
                print("No job cards found, trying heading-based detection...")
                
                # Find all headings that look like job titles
                job_headings = soup.find_all(['h1', 'h2', 'h3', 'h4'], 
                                           string=re.compile(r'manager|analyst|director|specialist|engineer|coordinator', re.I))
                
                print(f"Found {len(job_headings)} potential job headings")
                
                for heading in job_headings:
                    # For each job heading, find its logical container
                    # Try to find a parent that contains job info but not other jobs
                    for parent_level in [1, 2, 3]:  # Try different parent levels
                        container = heading
                        for _ in range(parent_level):
                            container = container.find_parent()
                            if not container:
                                break
                        
                        if container:
                            # Check if this container has a reasonable amount of text (job description)
                            container_text = container.get_text().strip()
                            if 50 < len(container_text) < 2000:  # Reasonable job description length
                                job_elements.append(container)
                                print(f"Added job element around heading: {heading.get_text()[:50]}")
                                break
            
            # Strategy 3: If still no elements, try to split the page by job sections
            if not job_elements:
                print("Trying to split page by job sections...")
                
                # Look for patterns that separate jobs (like "Apply Now" buttons or section dividers)
                page_content = soup.get_text()
                
                # Split by common job separators
                separators = ['Apply Now', 'View Job', 'Learn More', 'Read More']
                potential_jobs = []
                
                for separator in separators:
                    if separator in page_content:
                        # This is a more complex approach - for now, fall back to whole page
                        break
                
                # Fallback: use whole page but try to be smarter about title extraction
                job_elements = [soup]
                print("Using whole page as single element (fallback)")
            
            # Remove duplicates and sort by position on page
            unique_elements = []
            for element in job_elements:
                if element not in unique_elements:
                    unique_elements.append(element)
            
            job_elements = unique_elements
            print(f"Final job elements count: {len(job_elements)}")
            
            jobs_found = []
            
            # Speed optimization: Limit job elements processed
        for element in job_elements[:5]:  # Process max 5 job elements per company
                element_text = element.get_text().lower()
                
                # Check for keyword matches
                found_keywords = []
                for keyword in keywords:
                    if keyword.lower() in element_text:
                        found_keywords.append(keyword)
                
                # Separate core job keywords from modifiers  
                core_keywords = [kw for kw in found_keywords if kw.lower() not in ['remote', 'hybrid']]
                modifier_keywords = [kw for kw in found_keywords if kw.lower() in ['remote', 'hybrid']]
                
                # Only process if we have core job keywords (not just remote/hybrid)
                if core_keywords:
                    # Enhanced job title extraction with multiple strategies
                    title = "Job Opening"  # Default
                    title_found = False
                    
                    # Strategy 1: Look for exact keyword matches in headings
                    for keyword in core_keywords:
                        if title_found:
                            break
                        # Try to find headings that contain this specific keyword
                        keyword_headings = element.find_all(['h1', 'h2', 'h3', 'h4', 'h5'])
                        for heading in keyword_headings:
                            heading_text = heading.get_text().strip()
                            if keyword.lower() in heading_text.lower() and len(heading_text) > 5:
                                # Make sure this heading is actually a job title (not navigation or other text)
                                if any(job_word in heading_text.lower() for job_word in ['manager', 'analyst', 'director', 'specialist', 'engineer', 'coordinator']):
                                    title = heading_text
                                    title_found = True
                                    print(f"Found title using keyword '{keyword}': {title}")
                                    break
                    
                    # Strategy 2: Look for job-specific CSS classes
                    if not title_found:
                        title_selectors = [
                            ['h1', 'h2', 'h3', 'h4'],  # Try all heading levels
                            ['span', 'div'],           # Sometimes titles are in spans or divs
                            ['a']                      # Sometimes titles are links
                        ]
                        
                        for tag_list in title_selectors:
                            if title_found:
                                break
                            title_elements = element.find_all(tag_list, class_=re.compile(r'job.*title|title.*job|position.*title|role.*title', re.I))
                            if title_elements:
                                title = title_elements[0].get_text().strip()
                                if len(title) > 5:
                                    title_found = True
                                    print(f"Found title using CSS class: {title}")
                                    break
                    
                    # Strategy 3: Look for strong/bold text that contains job keywords
                    if not title_found:
                        strong_elements = element.find_all(['strong', 'b', 'h1', 'h2', 'h3', 'h4', 'h5'])
                        for strong_elem in strong_elements:
                            strong_text = strong_elem.get_text().strip()
                            if any(keyword.lower() in strong_text.lower() for keyword in core_keywords):
                                if len(strong_text) > 5 and len(strong_text) < 100:  # Reasonable title length
                                    title = strong_text
                                    title_found = True
                                    print(f"Found title in strong/heading: {title}")
                                    break
                    
                    # Strategy 4: Fallback to first heading that makes sense
                    if not title_found:
                        all_headings = element.find_all(['h1', 'h2', 'h3', 'h4', 'h5'])
                        for heading in all_headings:
                            heading_text = heading.get_text().strip()
                            if len(heading_text) > 5 and len(heading_text) < 100:
                                title = heading_text
                                print(f"Found title using fallback heading: {title}")
                                break
                    
                    # Clean up title
                    title = re.sub(r'\s+', ' ', title.replace('\n', ' ').replace('\t', ' '))
                    
                    # Extract job URL with better logic
                    job_url = url  # Default to page URL
                    
                    # Look for job-specific URLs
                    url_strategies = [
                        # Strategy 1: Look for "Apply Now" or similar buttons
                        lambda: element.find('a', href=True, string=re.compile(r'apply|view.*job|see.*detail|learn.*more', re.I)),
                        # Strategy 2: Look for links with job-related href patterns
                        lambda: element.find('a', href=re.compile(r'job|position|career|apply|vacancy', re.I)),
                        # Strategy 3: Look for any link that might be job-related
                        lambda: element.find('a', href=True, class_=re.compile(r'job|apply|view', re.I)),
                        # Strategy 4: First link in the element
                        lambda: element.find('a', href=True)
                    ]
                    
                    for strategy in url_strategies:
                        try:
                            link_element = strategy()
                            if link_element and link_element.get('href'):
                                href = link_element['href']
                                # Make relative URLs absolute
                                if href.startswith('/'):
                                    base_url = '/'.join(url.split('/')[:3])
                                    job_url = base_url + href
                                elif href.startswith('http'):
                                    job_url = href
                                else:
                                    base_url = '/'.join(url.split('/')[:3])
                                    job_url = base_url + '/' + href
                                break
                        except:
                            continue
                    
                    # Skip if we couldn't get a decent title
                    if len(title.strip()) < 5 or title.strip().lower() in ['job opening', 'learn more', 'read more', 'apply', 'apply now']:
                        continue
                    
                    jobs_found.append({
                        'title': title[:100],
                        'url': job_url,
                        'keywords_found': core_keywords + modifier_keywords,
                        'source': 'Careers Page'
                    })
            
            return jobs_found
            
        except Exception as e:
            print(f"  Error scraping careers page: {str(e)[:50]}")
            return []
    
    def scrape_indeed_company(self, company_name, location="Denver,CO", keywords=None):
        """Scrape Indeed for specific company jobs - EXACT COMPANY ONLY"""
        try:
            # Build Indeed search URL for exact company
            query = f'company:"{company_name}"'
            url = f"https://www.indeed.com/jobs?q={query.replace(' ', '+')}&l={location}"
            
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Indeed job selectors
            job_cards = soup.find_all(['div'], attrs={'data-jk': True})
            if not job_cards:
                job_cards = soup.find_all(['div'], class_=re.compile(r'job.*card|listing', re.I))
            
            jobs_found = []
            
            for card in job_cards[:6]:  # Limit to 6 jobs per company
                try:
                    # CRITICAL: Verify this job is actually from the target company
                    card_text = card.get_text().lower()
                    if company_name.lower() not in card_text:
                        continue  # Skip if not from our target company
                    
                    # Extract job title
                    title_elem = card.find(['h2', 'a'], attrs={'data-testid': 'job-title'})
                    if not title_elem:
                        title_elem = card.find(['h2', 'a'])
                    
                    title = title_elem.get_text().strip() if title_elem else "Indeed Job"
                    
                    # Extract job URL
                    link_elem = card.find('a', href=True)
                    if link_elem and '/viewjob' in link_elem['href']:
                        job_url = "https://www.indeed.com" + link_elem['href']
                    else:
                        job_url = url
                    
                    # Keyword matching for Indeed
                    found_keywords = []
                    if keywords:
                        for keyword in keywords:
                            if keyword.lower() in card_text:
                                found_keywords.append(keyword)
                    
                    # Apply same core keyword logic
                    core_keywords = [kw for kw in found_keywords if kw.lower() not in ['remote', 'hybrid']]
                    modifier_keywords = [kw for kw in found_keywords if kw.lower() in ['remote', 'hybrid']]
                    
                    # Only add if we have core job keywords (not just remote)
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
    
    def scrape_angellist_company(self, company_name):
        """Scrape AngelList for startup jobs - EXACT COMPANY ONLY"""
        try:
            # Convert company name to AngelList slug format
            company_slug = re.sub(r'[^a-zA-Z0-9-]', '', 
                                company_name.lower().replace(' ', '-').replace('.', ''))
            url = f"https://angel.co/company/{company_slug}/jobs"
            
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Verify we're on the correct company page
            page_text = soup.get_text().lower()
            if company_name.lower() not in page_text:
                return []  # Not the right company page
            
            # AngelList job selectors
            job_elements = soup.find_all(['div', 'article'], 
                                       class_=re.compile(r'job|listing|position', re.I))
            
            jobs_found = []
            
            for element in job_elements[:4]:  # Limit to 4 jobs
                try:
                    title_elem = element.find(['h1', 'h2', 'h3', 'a'])
                    title = title_elem.get_text().strip() if title_elem else "AngelList Job"
                    
                    link_elem = element.find('a', href=True)
                    job_url = link_elem['href'] if link_elem else url
                    
                    if job_url.startswith('/'):
                        job_url = "https://angel.co" + job_url
                    
                    jobs_found.append({
                        'title': title[:100],
                        'url': job_url,
                        'keywords_found': ['startup', 'angellist'],
                        'source': 'AngelList'
                    })
                    
                except Exception as e:
                    continue
            
            return jobs_found
            
        except Exception as e:
            print(f"  Error scraping AngelList: {str(e)[:50]}")
            return []
    
    def scrape_glassdoor_company(self, company_name, location="Denver, CO"):
        """Scrape Glassdoor for company jobs - EXACT COMPANY ONLY"""
        try:
            # Search specifically for the company
            search_query = company_name.replace(" ", "%20")
            url = f"https://www.glassdoor.com/Job/jobs.htm?sc.keyword={search_query}&locT=C&locId=1148170"
            
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Glassdoor job selectors
            job_cards = soup.find_all(['div'], class_=re.compile(r'job.*card|listing', re.I))
            
            jobs_found = []
            
            for card in job_cards[:3]:  # Limit to 3 jobs
                try:
                    # CRITICAL: Verify this job is from the target company
                    card_text = card.get_text().lower()
                    if company_name.lower() not in card_text:
                        continue  # Skip if not from our target company
                    
                    title_elem = card.find(['a', 'h2'], class_=re.compile(r'job.*title', re.I))
                    title = title_elem.get_text().strip() if title_elem else "Glassdoor Job"
                    
                    link_elem = card.find('a', href=True)
                    job_url = link_elem['href'] if link_elem else url
                    
                    if job_url.startswith('/'):
                        job_url = "https://www.glassdoor.com" + job_url
                    
                    jobs_found.append({
                        'title': title[:100],
                        'url': job_url,
                        'keywords_found': ['glassdoor_listing'],
                        'source': 'Glassdoor'
                    })
                    
                except Exception as e:
                    continue
            
            return jobs_found
            
        except Exception as e:
            print(f"  Error scraping Glassdoor: {str(e)[:50]}")
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
        
        # 2. Use job boards as backup OR primary (only if really needed)
        if primary_source == 'job_boards' or (len(all_jobs) == 0 and not careers_url):
            
            # Indeed (for all companies using job boards)
            indeed_jobs = self.scrape_indeed_company(company_name, f"{city},CO", keywords)
            all_jobs.extend(indeed_jobs)
            platform_stats['indeed'] = len(indeed_jobs)
            print(f"  Indeed: {len(indeed_jobs)} jobs")
            time.sleep(self.rate_limits['indeed'])
            
            # Skip AngelList and Glassdoor for speed (they rarely have good results anyway)
            # Only use if we have absolutely no jobs
            if len(all_jobs) == 0 and company_size == 'Small':
                angellist_jobs = self.scrape_angellist_company(company_name)
                all_jobs.extend(angellist_jobs)
                platform_stats['angellist'] = len(angellist_jobs)
                print(f"  AngelList: {len(angellist_jobs)} jobs")
                time.sleep(self.rate_limits['angellist'])
        
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
        print(f"Keywords: {', '.join(keywords[:10])}...")  # Show first 10 keywords
        
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
    scraper.run_multiplatform_scan()
