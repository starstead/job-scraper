import requests
from bs4 import BeautifulSoup
import time
import random
from urllib.parse import urljoin, urlparse
import csv
import pandas as pd
from dataclasses import dataclass
from typing import List, Optional, Dict, Set
import re
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class JobListing:
    title: str
    company: str
    url: str
    location: str = ""
    description: str = ""
    source: str = "careers"
    date_found: str = ""

@dataclass
class Company:
    name: str
    size: str
    careers_url: str
    indeed_search: str = ""

class MultiplatformJobScraper:
    def __init__(self, config_file: str = "config.json"):
        self.load_config(config_file)
        self.setup_session()
        self.existing_jobs = set()
        
        # Enhanced career page patterns
        self.career_paths = [
            '/careers/', '/careers/jobs/', '/careers/job-openings/', '/careers/open-positions/',
            '/careers/opportunities/', '/careers/current-openings/', '/careers/join-us/',
            '/jobs/', '/join-us/', '/work-with-us/', '/opportunities/', '/job-opportunities/',
            '/open-roles/', '/current-openings/', '/employment/', '/hiring/', '/positions/',
            '/careers/search/', '/careers/browse/', '/career-opportunities/'
        ]
        
        # Third-party job board patterns
        self.job_board_patterns = {
            'greenhouse.io': 'greenhouse',
            'lever.co': 'lever', 
            'bamboohr.com': 'bamboohr',
            'workday.com': 'workday',
            'smartrecruiters.com': 'smartrecruiters',
            'jobvite.com': 'jobvite',
            'icims.com': 'icims',
            'successfactors.com': 'successfactors',
            'applytojob.com': 'applytojob',
            'myworkdayjobs.com': 'workday'
        }
        
        # Enhanced selectors for different job board types
        self.job_selectors = {
            'greenhouse': {
                'container': '[data-qa="opening"], .opening',
                'title': '[data-qa="opening-title"], .opening a',
                'location': '[data-qa="opening-location"], .location'
            },
            'lever': {
                'container': '.posting',
                'title': '.posting-title a, .posting-name',
                'location': '.posting-location'
            },
            'bamboohr': {
                'container': '[data-qa="job-listing"], .BambooHR-ATS-Jobs-Item',
                'title': '[data-qa="job-title"], .BambooHR-ATS-Jobs-Item-Name a',
                'location': '[data-qa="job-location"], .BambooHR-ATS-Jobs-Item-Location'
            },
            'workday': {
                'container': '[data-automation-id="jobTitle"], .css-1f7j1dc',
                'title': '[data-automation-id="jobTitle"] a, h3 a',
                'location': '[data-automation-id="locations"]'
            },
            'generic': {
                'container': [
                    '.job', '.position', '.opening', '.career', '.role', '.opportunity',
                    '[class*="job"]', '[class*="position"]', '[class*="career"]',
                    '[data-testid*="job"]', '[data-qa*="job"]'
                ],
                'title': [
                    'h1', 'h2', 'h3', 'h4', 'h5', 'h6', '.title', '.job-title',
                    '[class*="title"]', 'a', 'strong'
                ],
                'location': [
                    '.location', '.city', '.office', '[class*="location"]',
                    '[class*="city"]', '[class*="office"]'
                ]
            }
        }

    def load_config(self, config_file: str):
        """Load configuration from JSON file"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            self.pm_keywords = config.get('keywords', [
                'product manager', 'product management', 'senior product manager',
                'principal product manager', 'group product manager', 'associate product manager',
                'lead product manager', 'director of product', 'head of product',
                'product owner', 'product lead', 'product strategy', 'vp of product',
                'chief product officer', 'cpo'
            ])
            
            # Make keywords case-insensitive regex patterns
            self.pm_patterns = [re.compile(keyword, re.IGNORECASE) for keyword in self.pm_keywords]
            
        except FileNotFoundError:
            logger.warning(f"Config file {config_file} not found. Using default keywords.")
            self.pm_keywords = ['product manager', 'senior product manager', 'principal product manager']
            self.pm_patterns = [re.compile(keyword, re.IGNORECASE) for keyword in self.pm_keywords]

    def setup_session(self):
        """Set up requests session with retry strategy and rotating user agents"""
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0'
        ]
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )

    def get_session(self):
        """Create a new session with random user agent"""
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=3)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        return session

    def load_companies(self, csv_file: str) -> List[Company]:
        """Load companies from CSV file"""
        companies = []
        try:
            df = pd.read_csv(csv_file)
            for _, row in df.iterrows():
                companies.append(Company(
                    name=row['Company'],
                    size=row.get('Size', 'Unknown'),
                    careers_url=row['Careers URL'],
                    indeed_search=row.get('Indeed Search', '')
                ))
            logger.info(f"✅ Loaded {len(companies)} companies from {csv_file}")
        except Exception as e:
            logger.error(f"Error loading companies: {e}")
        return companies

    def load_existing_jobs(self, notion_data=None):
        """Load existing jobs to avoid duplicates"""
        # This would integrate with your Notion API
        # For now, we'll use a simple set
        self.existing_jobs = set()
        logger.info(f"Found {len(self.existing_jobs)} existing jobs in Notion")

    def discover_career_urls(self, base_url: str, company_name: str) -> List[str]:
        """Discover actual career page URLs with enhanced detection"""
        discovered_urls = []
        session = self.get_session()
        
        try:
            logger.debug(f"Discovering career URLs for {company_name} from {base_url}")
            
            # Try the base URL first
            response = session.get(base_url, timeout=15, allow_redirects=True)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Check if current page has jobs
                if self.has_job_listings(soup):
                    discovered_urls.append(response.url)
                
                # Look for job-related links
                job_links = self.find_job_links(soup, base_url)
                discovered_urls.extend(job_links)
            
            # Try common career path variations
            base_domain = f"https://{urlparse(base_url).netloc}"
            for path in self.career_paths:
                try_url = base_domain + path
                if try_url not in [base_url] + discovered_urls:
                    try:
                        response = session.get(try_url, timeout=10, allow_redirects=True)
                        if response.status_code == 200:
                            soup = BeautifulSoup(response.content, 'html.parser')
                            if self.has_job_listings(soup):
                                discovered_urls.append(response.url)
                                logger.debug(f"Found jobs at: {response.url}")
                    except:
                        continue
                        
        except Exception as e:
            logger.debug(f"Error discovering URLs for {company_name}: {e}")
        
        # Remove duplicates and return
        unique_urls = list(dict.fromkeys(discovered_urls))  # Preserves order
        return unique_urls

    def find_job_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find links that likely lead to job listings"""
        job_links = []
        
        job_link_keywords = [
            'job', 'career', 'opening', 'position', 'opportunity', 'role',
            'hiring', 'join', 'work', 'employment', 'vacancy', 'apply'
        ]
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text().lower().strip()
            
            # Skip fragments and emails
            if href.startswith('#') or href.startswith('mailto:'):
                continue
                
            full_url = urljoin(base_url, href)
            
            # Check for job-related keywords in text or URL
            if (any(keyword in link_text for keyword in job_link_keywords) or 
                any(keyword in href.lower() for keyword in job_link_keywords)):
                job_links.append(full_url)
                
            # Check for third-party job boards
            if any(board in href for board in self.job_board_patterns.keys()):
                job_links.append(full_url)
        
        return job_links

    def has_job_listings(self, soup: BeautifulSoup) -> bool:
        """Enhanced detection of job listings on page"""
        # Check for PM-specific content
        pm_content = soup.find_all(string=self.pm_patterns)
        if pm_content:
            return True
            
        # Check for common job listing structures
        job_indicators = [
            soup.find_all(class_=re.compile(r'job|position|opening|career|role', re.I)),
            soup.find_all('div', {'data-qa': re.compile(r'job|opening', re.I)}),
            soup.find_all('div', {'data-testid': re.compile(r'job|position', re.I)}),
            soup.find_all(['button', 'a'], string=re.compile(r'apply|view job|see details', re.I))
        ]
        
        return any(len(indicators) > 0 for indicators in job_indicators)

    def detect_job_board_type(self, url: str, soup: BeautifulSoup) -> str:
        """Detect which type of job board this is"""
        # Check URL for known patterns
        for pattern, board_type in self.job_board_patterns.items():
            if pattern in url:
                return board_type
        
        # Check page content for indicators
        if soup.find(attrs={'data-qa': True}):
            if 'greenhouse' in str(soup):
                return 'greenhouse'
            elif 'bamboohr' in str(soup).lower():
                return 'bamboohr'
        
        if soup.find(attrs={'data-automation-id': True}):
            return 'workday'
            
        if soup.find(class_=re.compile(r'posting', re.I)):
            return 'lever'
        
        return 'generic'

    def extract_jobs_from_page(self, url: str, company: Company) -> List[JobListing]:
        """Extract job listings from a page with smart detection"""
        jobs = []
        session = self.get_session()
        
        try:
            logger.debug(f"Extracting jobs from: {url}")
            response = session.get(url, timeout=20, allow_redirects=True)
            if response.status_code != 200:
                logger.warning(f"Failed to fetch {url}: {response.status_code}")
                return jobs
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Detect job board type
            board_type = self.detect_job_board_type(url, soup)
            logger.debug(f"Detected job board type: {board_type}")
            
            # Use appropriate extraction method
            if board_type in self.job_selectors:
                jobs = self.extract_with_selectors(soup, company, url, board_type)
            else:
                jobs = self.extract_with_generic_method(soup, company, url)
            
            # Filter for PM roles
            pm_jobs = [job for job in jobs if self.is_product_manager_role(job.title)]
            
            if pm_jobs:
                logger.info(f"  ✅ Found {len(pm_jobs)} PM jobs")
            
            return pm_jobs
                
        except Exception as e:
            logger.error(f"Error extracting jobs from {url}: {e}")
            return jobs

    def extract_with_selectors(self, soup: BeautifulSoup, company: Company, url: str, board_type: str) -> List[JobListing]:
        """Extract jobs using board-specific selectors"""
        jobs = []
        selectors = self.job_selectors[board_type]
        
        try:
            # Find job containers
            containers = soup.select(selectors['container'])
            
            for container in containers:
                # Extract title
                title_elem = container.select_one(selectors['title'])
                if not title_elem:
                    continue
                    
                title = title_elem.get_text().strip()
                if not title:
                    continue
                
                # Extract job URL
                job_url = url
                if title_elem.name == 'a' and title_elem.get('href'):
                    job_url = urljoin(url, title_elem['href'])
                else:
                    link = container.find('a', href=True)
                    if link:
                        job_url = urljoin(url, link['href'])
                
                # Extract location
                location = ""
                if 'location' in selectors:
                    loc_elem = container.select_one(selectors['location'])
                    if loc_elem:
                        location = loc_elem.get_text().strip()
                
                jobs.append(JobListing(
                    title=title,
                    company=company.name,
                    url=job_url,
                    location=location,
                    description=container.get_text().strip()[:300],
                    source=f"careers_{board_type}"
                ))
                
        except Exception as e:
            logger.error(f"Error with {board_type} selectors: {e}")
        
        return jobs

    def extract_with_generic_method(self, soup: BeautifulSoup, company: Company, url: str) -> List[JobListing]:
        """Generic extraction method for unknown job boards"""
        jobs = []
        
        try:
            # Try multiple strategies
            job_elements = []
            
            # Strategy 1: Find elements containing PM keywords
            for pattern in self.pm_patterns:
                elements = soup.find_all(string=pattern)
                for element in elements:
                    # Find the job container
                    container = element.parent
                    for _ in range(5):  # Look up to 5 levels up
                        if container and container.name in ['div', 'li', 'section', 'article', 'tr']:
                            if len(container.get_text().strip()) > 50:  # Reasonable content length
                                job_elements.append(container)
                                break
                        container = container.parent if container else None
            
            # Strategy 2: Look for common job container patterns
            for selector_list in self.job_selectors['generic']['container']:
                containers = soup.select(selector_list)
                for container in containers:
                    if any(pattern.search(container.get_text()) for pattern in self.pm_patterns):
                        job_elements.append(container)
            
            # Extract job details from found elements
            for element in job_elements:
                job = self.extract_job_from_element(element, company, url)
                if job:
                    jobs.append(job)
            
        except Exception as e:
            logger.error(f"Error with generic extraction: {e}")
        
        return jobs

    def extract_job_from_element(self, element, company: Company, base_url: str) -> Optional[JobListing]:
        """Extract job details from a single element"""
        try:
            # Find title using multiple strategies
            title = ""
            for selector_list in self.job_selectors['generic']['title']:
                title_elem = element.select_one(selector_list)
                if title_elem:
                    title = title_elem.get_text().strip()
                    if len(title) > 5 and len(title) < 200:  # Reasonable title length
                        break
            
            if not title:
                # Fallback: use the first line of text
                text_lines = [line.strip() for line in element.get_text().split('\n') if line.strip()]
                if text_lines:
                    title = text_lines[0]
            
            if not title or not self.is_product_manager_role(title):
                return None
            
            # Find job URL
            job_url = base_url
            link = element.find('a', href=True)
            if link:
                job_url = urljoin(base_url, link['href'])
            
            # Find location
            location = ""
            for selector_list in self.job_selectors['generic']['location']:
                loc_elem = element.select_one(selector_list)
                if loc_elem:
                    location = loc_elem.get_text().strip()
                    break
            
            return JobListing(
                title=title,
                company=company.name,
                url=job_url,
                location=location,
                description=element.get_text().strip()[:300],
                source="careers"
            )
            
        except Exception as e:
            logger.debug(f"Error extracting job details: {e}")
            return None

    def is_product_manager_role(self, title: str) -> bool:
        """Check if job title matches PM keywords"""
        return any(pattern.search(title) for pattern in self.pm_patterns)

    def scrape_indeed(self, company: Company) -> List[JobListing]:
        """Scrape Indeed for additional jobs (placeholder for your existing logic)"""
        # Implement your existing Indeed scraping logic here
        jobs = []
        
        if not company.indeed_search:
            return jobs
        
        try:
            # Your Indeed scraping code would go here
            logger.debug(f"Scraping Indeed for {company.name}")
            # ... Indeed implementation
            
        except Exception as e:
            logger.error(f"Error scraping Indeed for {company.name}: {e}")
        
        return jobs

    def scrape_company(self, company: Company) -> List[JobListing]:
        """Scrape all jobs for a single company"""
        logger.info(f"📊 Scanning {company.name} ({company.size})...")
        all_jobs = []
        
        try:
            # Scrape career pages
            career_urls = self.discover_career_urls(company.careers_url, company.name)
            
            if not career_urls:
                logger.warning(f"No career URLs found for {company.name}")
                career_urls = [company.careers_url]  # Fallback
            
            logger.debug(f"Found {len(career_urls)} career page(s) to check")
            
            for url in career_urls:
                try:
                    jobs = self.extract_jobs_from_page(url, company)
                    all_jobs.extend(jobs)
                    
                    # Respectful delay
                    time.sleep(random.uniform(1, 2))
                    
                except Exception as e:
                    logger.error(f"Error with {url}: {e}")
            
            # Scrape Indeed if enabled
            indeed_jobs = self.scrape_indeed(company)
            all_jobs.extend(indeed_jobs)
            
            # Remove duplicates
            unique_jobs = self.remove_duplicates(all_jobs)
            
            total_jobs = len(unique_jobs)
            if total_jobs > 0:
                logger.info(f"  ✅ Total: {total_jobs} jobs")
            else:
                logger.info("  No PM jobs found")
            
            return unique_jobs
            
        except Exception as e:
            logger.error(f"Error scraping {company.name}: {e}")
            return []

    def remove_duplicates(self, jobs: List[JobListing]) -> List[JobListing]:
        """Remove duplicate jobs based on title and company"""
        seen = set()
        unique_jobs = []
        
        for job in jobs:
            key = (job.title.lower().strip(), job.company.lower().strip())
            if key not in seen:
                seen.add(key)
                unique_jobs.append(job)
        
        return unique_jobs

    def save_results(self, jobs: List[JobListing], filename: str = "product_manager_jobs.csv"):
        """Save jobs to CSV"""
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as file:
                import csv
                writer = csv.writer(file)
                writer.writerow(['Company', 'Title', 'Location', 'URL', 'Source', 'Description'])
                
                for job in jobs:
                    writer.writerow([
                        job.company,
                        job.title,
                        job.location,
                        job.url,
                        job.source,
                        job.description[:200]
                    ])
            
            logger.info(f"Jobs saved to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")

    def run_scan(self, companies_file: str = "companies_final_ready.csv", max_workers: int = 5):
        """Run the complete scan with optional threading"""
        logger.info("🚀 Starting multi-platform job scan...")
        
        # Load companies and existing jobs
        companies = self.load_companies(companies_file)
        self.load_existing_jobs()
        
        if not companies:
            logger.error("No companies loaded. Exiting.")
            return
        
        logger.info(f"Keywords: {', '.join(self.pm_keywords[:10])}...")
        
        all_jobs = []
        
        if max_workers > 1:
            # Multi-threaded scanning
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_company = {
                    executor.submit(self.scrape_company, company): company 
                    for company in companies
                }
                
                for future in as_completed(future_to_company):
                    company = future_to_company[future]
                    try:
                        jobs = future.result()
                        all_jobs.extend(jobs)
                    except Exception as e:
                        logger.error(f"Error processing {company.name}: {e}")
        else:
            # Single-threaded scanning
            for company in companies:
                jobs = self.scrape_company(company)
                all_jobs.extend(jobs)
                time.sleep(random.uniform(2, 4))  # Longer delay between companies
        
        # Final results
        logger.info("\n🎉 MULTI-PLATFORM SCAN COMPLETE!")
        logger.info(f"Companies scanned: {len(companies)}")
        logger.info(f"Total PM jobs found: {len(all_jobs)}")
        
        if all_jobs:
            # Save results
            self.save_results(all_jobs)
            
            # Show breakdown by company
            company_counts = {}
            for job in all_jobs:
                company_counts[job.company] = company_counts.get(job.company, 0) + 1
            
            logger.info("\nJobs by company:")
            for company, count in sorted(company_counts.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {company}: {count} jobs")
        
        return all_jobs

# Main execution
if __name__ == "__main__":
    # Create and configure scraper
    scraper = MultiplatformJobScraper()
    
    # Run the scan
    jobs = scraper.run_scan(
        companies_file="companies_final_ready.csv",
        max_workers=3  # Adjust based on your needs
    )
    
    print(f"\nScan complete! Found {len(jobs)} total PM jobs.")
