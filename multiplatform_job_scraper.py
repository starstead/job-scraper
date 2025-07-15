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
            return config.get('keywords', [])
        except:
            # Enhanced default keywords - your product/project manager list with variations
            return [
                "product manager", "Product Manager", "senior product manager", "Senior Product Manager",
                "associate product manager", "Associate Product Manager", "principal product manager", 
                "Principal Product Manager", "group product manager", "Group Product Manager",
                "product owner", "Product Owner", "head of product", "Head of Product",
                "director of product management", "Director of Product Management",
                "product management", "Product Management", "project manager", "Project Manager",
                "senior project manager", "Senior Project Manager", "program manager", "Program Manager",
                "senior program manager", "Senior Program Manager", "technical project manager",
                "Technical Project Manager", "technical program manager", "Technical Program Manager",
                "portfolio manager", "Portfolio Manager", "implementation manager", "Implementation Manager",
                "implementation project manager", "PMO manager", "PMO Manager",
                "manager of program management", "manager of project management",
                "director of program management", "director of project management", 
                "strategy manager", "Strategy Manager", "business analyst", "Business Analyst",
                "integration product manager", "product manager mergers and acquisitions",
                "M&A product manager", "post-merger integration manager", "integration program manager",
                "customer success operations manager", "customer success strategy manager",
                "product operations manager", "customer experience manager",
                "product-led customer success manager", "project management", "Project Management",
                "program management", "Program Management"
            ]
    
    def scrape_careers_page(self, url, keywords, company_name):
        """Scrape company careers page (primary method)"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find job listings with various selectors
            job_elements = soup.find_all(['div', 'article', 'section', 'li'], 
                                       class_=re.compile(r'job|position|career|opening|listing', re.I))
            
            # If no specific job elements found, search entire page
            if not job_elements:
                job_elements = [soup]
            
            jobs_found = []
            
            for element in job_elements[:15]:  # Limit to 15 elements per page
                element_text = element.get_text()
                
                # Enhanced keyword matching
                found_keywords = self.check_keyword_match(element_text, keywords)
                
                # Categorize keywords  
                core_keywords, modifier_keywords = self.categorize_keywords(found_keywords)
        """Enhanced keyword matching with case-insensitive and flexible matching"""
        text_lower = text.lower()
        found_keywords = []
        
        for keyword in keywords:
            keyword_lower = keyword.lower()
            
            # Method 1: Exact match (case-insensitive)
            if keyword_lower in text_lower:
                found_keywords.append(keyword)
                continue
            
            # Method 2: Word boundary matching for compound terms
            # This helps catch "Product Management" even if keyword is "product manager"
            keyword_words = keyword_lower.split()
            if len(keyword_words) >= 2:
                # Check if all words of the keyword appear in the text
                all_words_found = all(word in text_lower for word in keyword_words)
                if all_words_found:
                    # Additional check: words should be reasonably close to each other
                    keyword_pattern = r'\b' + r'\W+'.join(re.escape(word) for word in keyword_words) + r'\b'
                    if re.search(keyword_pattern, text_lower):
                        found_keywords.append(keyword)
                        continue
        
        return found_keywords
    
    def categorize_keywords(self, found_keywords):
        """Categorize keywords into core job keywords vs modifiers"""
        # Modifiers that shouldn't count as core job requirements
        modifiers = ['remote', 'hybrid', 'work from home', 'telecommute', 'virtual']
        
        core_keywords = []
        modifier_keywords = []
        
        for keyword in found_keywords:
            keyword_lower = keyword.lower()
            if any(modifier in keyword_lower for modifier in modifiers):
                modifier_keywords.append(keyword)
            else:
                core_keywords.append(keyword)
        
        return core_keywords, modifier_keywords
                
                # Only process if we have core job keywords (not just remote/hybrid)
                if core_keywords:
                    # Better job title extraction
                    title = "Job Opening"  # Default
                    
                    # Try multiple title extraction strategies
                    title_strategies = [
                        # Strategy 1: Look for job-specific title classes
                        lambda: element.find(['h1', 'h2', 'h3'], class_=re.compile(r'job.*title|title.*job|position.*title', re.I)),
                        # Strategy 2: Look for any link with job-related text
                        lambda: element.find('a', string=re.compile(r'manager|engineer|analyst|director|specialist', re.I)),
                        # Strategy 3: Look for any heading with our keywords
                        lambda: next((h for h in element.find_all(['h1', 'h2', 'h3', 'h4', 'h5']) 
                                     if any(kw in h.get_text().lower() for kw in core_keywords)), None),
                        # Strategy 4: Look for any bold/strong text with keywords
                        lambda: next((b for b in element.find_all(['strong', 'b']) 
                                     if any(kw in b.get_text().lower() for kw in core_keywords)), None),
                        # Strategy 5: Generic heading
                        lambda: element.find(['h1', 'h2', 'h3', 'h4'])
                    ]
                    
                    for strategy in title_strategies:
                        try:
                            title_element = strategy()
                            if title_element and len(title_element.get_text().strip()) > 3:
                                title = title_element.get_text().strip()
                                # Clean up title
                                title = re.sub(r'\s+', ' ', title)  # Remove extra whitespace
                                title = title.replace('\n', ' ').replace('\t', ' ')
                                if len(title) > 5 and title != "Job Opening":
                                    break
                        except:
                            continue
                    
                    # Better job URL extraction
                    job_url = url  # Default to page URL
                    
                    # Try multiple URL extraction strategies
                    url_strategies = [
                        # Strategy 1: Look for apply/view job buttons
                        lambda: element.find('a', href=True, string=re.compile(r'apply|view.*job|see.*detail', re.I)),
                        # Strategy 2: Look for links with job-related classes
                        lambda: element.find('a', href=True, class_=re.compile(r'job.*link|apply|view.*job', re.I)),
                        # Strategy 3: Look for links with job-related href patterns
                        lambda: element.find('a', href=re.compile(r'job|position|career|apply|vacancy', re.I)),
                        # Strategy 4: Look for any link that contains our job title
                        lambda: element.find('a', href=True, string=re.compile(re.escape(title[:20]), re.I)) if len(title) > 10 else None,
                        # Strategy 5: Any link in the element
                        lambda: element.find('a', href=True)
                    ]
                    
                    for strategy in url_strategies:
                        try:
                            link_element = strategy()
                            if link_element and link_element.get('href'):
                                href = link_element['href']
                                # Prefer URLs that look like direct job links
                                if any(pattern in href.lower() for pattern in ['job', 'position', 'career', 'apply', 'vacancy']):
                                    job_url = href
                                    break
                                elif job_url == url:  # Use as fallback if we don't have anything better
                                    job_url = href
                        except:
                            continue
                    
                    # Clean and validate URL
                    if job_url and job_url != url:
                        # Make relative URLs absolute
                        if job_url.startswith('/'):
                            base_url = '/'.join(url.split('/')[:3])
                            job_url = base_url + job_url
                        elif job_url.startswith('http') == False:
                            base_url = '/'.join(url.split('/')[:3])
                            job_url = base_url + '/' + job_url
                    
                    # Skip if we couldn't get a decent title
                    if len(title.strip()) < 5 or title.strip().lower() in ['job opening', 'learn more', 'read more', 'apply']:
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
                    
                    # Improved keyword matching for Indeed
                    found_keywords = []
                    if keywords:
                        for keyword in keywords:
                            if keyword.lower() in card_text:
                                found_keywords.append(keyword)
                    
                    # Apply same core keyword logic
                    core_keywords = [kw for kw in found_keywords if kw not in ['remote', 'hybrid']]
                    modifier_keywords = [kw for kw in found_keywords if kw in ['remote', 'hybrid']]
                    
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
    
    def create_unique_id(self, company, title, url):
        """Create unique ID for duplicate detection - IMPROVED"""
        # Clean and normalize the inputs
        company_clean = re.sub(r'[^a-zA-Z0-9]', '', company.lower())
        title_clean = re.sub(r'[^a-zA-Z0-9]', '', title.lower())
        url_clean = url.split('?')[0].lower()  # Remove URL parameters
        
        unique_string = f"{company_clean}_{title_clean}_{url_clean}"
        return hashlib.md5(unique_string.encode()).hexdigest()[:16]
    
    def remove_duplicates_from_jobs(self, jobs_list):
        """Remove duplicates from jobs list before adding to Notion"""
        seen_jobs = {}
        unique_jobs = []
        
        for job in jobs_list:
            # Create multiple keys for comparison
            title_company_key = f"{job['company'].lower().strip()}_{job['title'].lower().strip()}"
            url_key = job['url'].split('?')[0].lower()  # Remove URL parameters
            
            # Check if we've seen this job before
            is_duplicate = False
            
            # Check by title + company
            if title_company_key in seen_jobs:
                is_duplicate = True
                print(f"  🔄 Removed duplicate (title+company): {job['title'][:50]}")
            
            # Check by URL (if it's a real job URL, not just the company page)
            elif url_key in seen_jobs and 'job' in url_key.lower():
                is_duplicate = True
                print(f"  🔄 Removed duplicate (URL): {job['title'][:50]}")
            
            if not is_duplicate:
                seen_jobs[title_company_key] = job
                seen_jobs[url_key] = job
                unique_jobs.append(job)
        
        return unique_jobs
    
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
        
        # 2. Use job boards as backup OR primary (based on strategy)
        # ONLY if careers page failed or company has no careers URL
        if primary_source == 'job_boards' or len(all_jobs) < 2:
            
            # Indeed (for all companies using job boards)
            indeed_jobs = self.scrape_indeed_company(company_name, f"{city},CO", keywords)
            all_jobs.extend(indeed_jobs)
            platform_stats['indeed'] = len(indeed_jobs)
            print(f"  Indeed: {len(indeed_jobs)} jobs")
            time.sleep(self.rate_limits['indeed'])
            
            # AngelList (for small companies)
            if company_size == 'Small':
                angellist_jobs = self.scrape_angellist_company(company_name)
                all_jobs.extend(angellist_jobs)
                platform_stats['angellist'] = len(angellist_jobs)
                print(f"  AngelList: {len(angellist_jobs)} jobs")
                time.sleep(self.rate_limits['angellist'])
            
            # Glassdoor (if we still haven't found enough jobs)
            if len(all_jobs) < 3:
                glassdoor_jobs = self.scrape_glassdoor_company(company_name, city)
                all_jobs.extend(glassdoor_jobs)
                platform_stats['glassdoor'] = len(glassdoor_jobs)
                print(f"  Glassdoor: {len(glassdoor_jobs)} jobs")
                time.sleep(self.rate_limits['glassdoor'])
        
        # Remove duplicates within this company's jobs
        all_jobs = self.remove_duplicates_from_jobs(all_jobs)
        
        # Add company metadata to all jobs
        for job in all_jobs:
            job['company'] = company_name
            job['industry'] = company_data.get('Industry', 'Unknown')
            job['company_size'] = company_size
            job['scan_date'] = datetime.now().strftime('%Y-%m-%d')
            job['unique_id'] = self.create_unique_id(company_name, job['title'], job['url'])
        
        print(f"  Final total: {len(all_jobs)} jobs")
        
        return all_jobs, platform_stats
    
    def get_existing_jobs(self, notion, database_id):
        """Get existing jobs from Notion to check for duplicates - ENHANCED"""
        try:
            existing_jobs = set()
            existing_titles = set()
            existing_company_title_combos = set()
            has_more = True
            start_cursor = None
            
            print("📋 Loading existing jobs from Notion to prevent duplicates...")
            
            while has_more:
                response = notion.databases.query(
                    database_id=database_id,
                    start_cursor=start_cursor,
                    page_size=100
                )
                
                for page in response['results']:
                    try:
                        # Get unique ID
                        unique_id = page['properties'].get('Unique ID', {}).get('rich_text', [])
                        if unique_id:
                            existing_jobs.add(unique_id[0]['plain_text'])
                        
                        # Get title and company for comprehensive duplicate checking
                        title = page['properties'].get('Job Title', {}).get('title', [])
                        company = page['properties'].get('Company', {}).get('rich_text', [])
                        
                        if title and company:
                            title_text = title[0]['plain_text'].lower().strip()
                            company_text = company[0]['plain_text'].lower().strip()
                            
                            # Multiple ways to track the same job
                            existing_titles.add(f"{company_text}_{title_text}")
                            
                            # Normalize for better matching
                            title_normalized = re.sub(r'[^a-zA-Z0-9\s]', '', title_text)
                            title_normalized = re.sub(r'\s+', ' ', title_normalized).strip()
                            
                            existing_company_title_combos.add(f"{company_text}|{title_normalized}")
                            
                            # Also check for partial title matches (in case title formatting changes)
                            title_keywords = title_normalized.split()
                            if len(title_keywords) >= 2:
                                title_short = ' '.join(title_keywords[:2])  # First two words
                                existing_company_title_combos.add(f"{company_text}|{title_short}")
                    except:
                        continue
                
                has_more = response['has_more']
                start_cursor = response.get('next_cursor')
            
            print(f"📋 Found {len(existing_jobs)} existing jobs to avoid duplicating")
            
            return existing_jobs, existing_titles, existing_company_title_combos
            
        except Exception as e:
            print(f"Error getting existing jobs: {e}")
            return set(), set(), set()
    
    def is_job_duplicate(self, job, existing_job_ids, existing_titles, existing_combos):
        """Comprehensive duplicate checking for a job"""
        company_lower = job['company'].lower().strip()
        title_lower = job['title'].lower().strip()
        
        # Method 1: Check unique ID
        if job['unique_id'] in existing_job_ids:
            return True, "unique_id"
        
        # Method 2: Check exact title + company combination
        title_company_key = f"{company_lower}_{title_lower}"
        if title_company_key in existing_titles:
            return True, "title_company"
        
        # Method 3: Check normalized title + company
        title_normalized = re.sub(r'[^a-zA-Z0-9\s]', '', title_lower)
        title_normalized = re.sub(r'\s+', ' ', title_normalized).strip()
        combo_key = f"{company_lower}|{title_normalized}"
        if combo_key in existing_combos:
            return True, "normalized_combo"
        
        # Method 4: Check partial title match (first 2 words)
        title_words = title_normalized.split()
        if len(title_words) >= 2:
            title_short = ' '.join(title_words[:2])
            short_combo_key = f"{company_lower}|{title_short}"
            if short_combo_key in existing_combos:
                return True, "partial_title"
        
        return False, None
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
        existing_titles = set()
        existing_combos = set()
        
        if notion_token and notion_database_id:
            notion = Client(auth=notion_token)
            existing_job_ids, existing_titles, existing_combos = self.get_existing_jobs(notion, notion_database_id)
            print(f"📋 Loaded existing jobs for duplicate prevention")
        else:
            print("Notion credentials not provided, skipping Notion sync")
        
        all_jobs = []
        total_stats = {'careers': 0, 'indeed': 0, 'angellist': 0, 'glassdoor': 0}
        companies_scanned = 0
        new_jobs_added = 0
        duplicates_skipped = 0
        
        # Full scan of all companies from YOUR CSV ONLY
        companies_to_scan = companies
        
        for company_data in companies_to_scan:
            try:
                company_jobs, platform_stats = self.scan_company_multiplatform(
                    company_data, keywords
                )
                
                # Process jobs for Notion (if configured)
                for job in company_jobs:
                    all_jobs.append(job)
                    
                    # Enhanced duplicate checking with detailed logging
                    if notion:
                        is_duplicate, duplicate_reason = self.is_job_duplicate(
                            job, existing_job_ids, existing_titles, existing_combos
                        )
                        
                        if not is_duplicate:
                            if self.add_job_to_notion(notion, notion_database_id, job):
                                new_jobs_added += 1
                                # Add to tracking sets to prevent duplicates within this run
                                existing_job_ids.add(job['unique_id'])
                                
                                company_lower = job['company'].lower().strip()
                                title_lower = job['title'].lower().strip()
                                title_company_key = f"{company_lower}_{title_lower}"
                                existing_titles.add(title_company_key)
                                
                                title_normalized = re.sub(r'[^a-zA-Z0-9\s]', '', title_lower)
                                title_normalized = re.sub(r'\s+', ' ', title_normalized).strip()
                                combo_key = f"{company_lower}|{title_normalized}"
                                existing_combos.add(combo_key)
                                
                                print(f"    ✅ Added to Notion: {job['title'][:50]}")
                            else:
                                print(f"    ❌ Failed to add: {job['title'][:50]}")
                        else:
                            duplicates_skipped += 1
                            print(f"    🔄 Duplicate skipped ({duplicate_reason}): {job['title'][:50]}")
                
                companies_scanned += 1
                
                # Update total stats
                for platform, count in platform_stats.items():
                    total_stats[platform] += count
                
                # Adaptive delay based on company size
                delay = 3 if company_data.get('Company_Size') == 'Large' else 5
                time.sleep(delay)
                
            except Exception as e:
                print(f"  Error processing {company_data['Company']}: {e}")
                continue
        
        # Save results
        self.save_results(all_jobs, total_stats, companies_scanned, new_jobs_added, duplicates_skipped)
        
        return all_jobs
    
    def save_results(self, all_jobs, stats, companies_scanned, new_jobs_added, duplicates_skipped):
        """Save multi-platform scan results"""
        os.makedirs('results', exist_ok=True)
        
        results = {
            'scan_date': datetime.now().isoformat(),
            'companies_scanned': companies_scanned,
            'total_jobs': len(all_jobs),
            'new_jobs_added_to_notion': new_jobs_added,
            'duplicates_skipped': duplicates_skipped,
            'platform_breakdown': stats,
            'jobs': all_jobs
        }
        
        with open('results/multiplatform_scan.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n🎉 MULTI-PLATFORM SCAN COMPLETE!")
        print(f"Companies scanned: {companies_scanned}")
        print(f"Total jobs found: {len(all_jobs)}")
        print(f"New jobs added to Notion: {new_jobs_added}")
        print(f"Duplicates skipped: {duplicates_skipped}")
        print(f"Platform breakdown:")
        for platform, count in stats.items():
            if count > 0:
                print(f"  {platform.title()}: {count} jobs")

if __name__ == "__main__":
    scraper = MultiPlatformJobScraper()
    scraper.run_multiplatform_scan()
