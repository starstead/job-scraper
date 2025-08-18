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
import ssl
import warnings
from urllib3.exceptions import InsecureRequestWarning
import os

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
    def __init__(self, config_file: str = "config_fixed.json"):
        self.load_config(config_file)
        self.setup_session()
        self.existing_jobs = set()
        
        # Enhanced career page patterns
        self.career_paths = [
            '/careers/', '/jobs/', '/careers/jobs/'  # Limited set to avoid timeouts
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
                    '.js-card', '.js-careers-page-job-list-item', '.job', '.position', '.opening', '.career', '.role', '.opportunity',
                    '[class*="job"]', '[class*="position"]', '[class*="career"]', '[class*="opening"]',
                    '[data-testid*="job"]', '[data-qa*="job"]', 'li', '.list-item'
                ],
                'title': [
                    '.js-job-list-opening-name', 'h3.rb-h3', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 
                    '.title', '.job-title', '.position-title', '.role-title', '.opening-title',
                    '[class*="title"]', '[class*="name"]', 'a[href*="job"]', 'a[href*="career"]',
                    'strong', 'b'
                ],
                'location': [
                    '.js-job-list-opening-loc', '.location', '.city', '.office', '[class*="location"]',
                    '[class*="city"]', '[class*="office"]', '[class*="loc"]'
                ]
            }
        }

    def load_config(self, config_file: str):
        """Load configuration from JSON file with better error handling"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            # Use ALL keywords from the JSON file
            self.job_keywords = config.get('keywords', [])
            
            # Make keywords case-insensitive regex patterns
            self.job_patterns = [re.compile(re.escape(keyword), re.IGNORECASE) for keyword in self.job_keywords]
            logger.info(f"✅ Loaded {len(self.job_keywords)} keywords from {config_file}")
            
            # Load settings if available
            self.settings = config.get('settings', {})
            
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Config file {config_file} not found or corrupted: {e}. Using default keywords.")
            # Fallback keywords if JSON file not found
            self.job_keywords = [
                'product manager', 'project manager', 'program manager', 'business analyst',
                'strategy manager', 'product owner', 'technical project manager'
            ]
            self.job_patterns = [re.compile(re.escape(keyword), re.IGNORECASE) for keyword in self.job_keywords]
            self.settings = {}

    def setup_session(self):
        """Set up requests session with retry strategy and rotating user agents"""
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0'
        ]

    def get_session(self):
        """Create a new session with random user agent and SSL/timeout fixes"""
        session = requests.Session()
        
        # Disable SSL warnings for problematic sites
        warnings.filterwarnings('ignore', category=InsecureRequestWarning)
        session.verify = False  # Disable SSL verification for problematic sites
        
        # Enhanced retry strategy
        retry_strategy = Retry(
            total=2,  # Reduced retries to avoid long timeouts
            backoff_factor=0.5,  # Faster backoff
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]  # Only retry GET requests
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
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
        """Load companies from CSV file with NaN value handling"""
        companies = []
        try:
            df = pd.read_csv(csv_file)
            logger.info(f"📊 CSV columns: {list(df.columns)}")
            logger.info(f"📊 CSV shape: {df.shape}")
            
            for _, row in df.iterrows():
                # Handle different possible column names flexibly
                name = row.get('Company', row.get('name', 'Unknown Company'))
                if pd.isna(name) or not str(name).strip():
                    continue  # Skip companies with no name
                
                name = str(name).strip()
                
                # Try multiple size column names
                size = (row.get('Size') or row.get('Company_Size') or 
                       row.get('company_size') or row.get('employees') or 'Unknown')
                
                # Handle careers URL with NaN checking
                careers_url = (row.get('Careers URL') or row.get('Careers Site URL') or 
                              row.get('careers_url') or row.get('website'))
                
                # Clean up NaN values and invalid URLs
                if pd.isna(careers_url) or careers_url == 'nan' or not str(careers_url).strip():
                    # Generate a likely careers URL from company name
                    clean_name = re.sub(r'[^a-zA-Z0-9]', '', name.lower())
                    careers_url = f'https://{clean_name}.com/careers'
                    logger.debug(f"Generated careers URL for {name}: {careers_url}")
                else:
                    careers_url = str(careers_url).strip()
                    # Ensure URL has proper scheme
                    if not careers_url.startswith(('http://', 'https://')):
                        careers_url = 'https://' + careers_url
                
                # Handle indeed search with NaN checking
                indeed_search = (row.get('Indeed Search') or row.get('Indeed_URL') or 
                               row.get('indeed_url'))
                
                if pd.isna(indeed_search) or indeed_search == 'nan' or not str(indeed_search).strip():
                    indeed_search = f'"{name}" product manager'
                else:
                    indeed_search = str(indeed_search).strip()
                
                companies.append(Company(
                    name=name,
                    size=str(size) if not pd.isna(size) else 'Unknown',
                    careers_url=careers_url,
                    indeed_search=indeed_search
                ))
                
            logger.info(f"✅ Loaded {len(companies)} companies from {csv_file}")
            
        except Exception as e:
            logger.error(f"Error loading companies: {e}")
            logger.error(f"Current working directory: {os.getcwd()}")
            if os.path.exists(csv_file):
                logger.error(f"CSV file exists but couldn't be read")
            else:
                logger.error(f"CSV file not found: {csv_file}")
        return companies

    def load_existing_jobs(self):
        """Load existing jobs to avoid duplicates"""
        # Simple set for tracking duplicates within this run
        self.existing_jobs = set()
        logger.info(f"Initialized job tracking")

    def discover_career_urls(self, base_url: str, company_name: str) -> List[str]:
        """Discover actual career page URLs with enhanced detection and timeout handling"""
        discovered_urls = []
        session = self.get_session()
        
        try:
            logger.debug(f"Discovering career URLs for {company_name} from {base_url}")
            
            # Skip obviously invalid URLs
            if not base_url or base_url == 'nan' or 'nan' in base_url:
                logger.debug(f"Skipping invalid URL for {company_name}: {base_url}")
                return []
            
            # Try the base URL first with timeout protection
            try:
                response = session.get(base_url, timeout=10, allow_redirects=True)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Check if current page has jobs
                    if self.has_job_listings(soup):
                        discovered_urls.append(response.url)
                    
                    # Look for job-related links
                    job_links = self.find_job_links(soup, base_url)
                    discovered_urls.extend(job_links[:3])  # Limit to first 3 to avoid timeouts
                    
            except (requests.exceptions.Timeout, requests.exceptions.SSLError, 
                    requests.exceptions.ConnectionError) as e:
                logger.debug(f"Connection issue with {base_url}: {e}")
                return []
            
            # Try common career path variations (limited to avoid timeouts)
            if len(discovered_urls) == 0:  # Only try if no URLs found yet
                try:
                    parsed_url = urlparse(base_url)
                    base_domain = f"https://{parsed_url.netloc}"
                except Exception:
                    base_domain = base_url
                
                for path in self.career_paths:
                    try_url = base_domain + path
                    if try_url not in [base_url] + discovered_urls:
                        try:
                            response = session.get(try_url, timeout=8, allow_redirects=True)
                            if response.status_code == 200:
                                soup = BeautifulSoup(response.content, 'html.parser')
                                if self.has_job_listings(soup):
                                    discovered_urls.append(response.url)
                                    logger.debug(f"Found jobs at: {response.url}")
                                    break  # Stop after finding first working URL
                        except Exception:
                            continue
                            
        except Exception as e:
            logger.debug(f"Error discovering URLs for {company_name}: {e}")
        
        # Remove duplicates and return limited set
        unique_urls = list(dict.fromkeys(discovered_urls))[:2]  # Max 2 URLs to avoid timeouts
        return unique_urls

    def find_job_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find links that likely lead to job listings"""
        job_links = []
        
        job_link_keywords = [
            'job', 'career', 'opening', 'position', 'opportunity', 'role',
            'hiring', 'join', 'work', 'employment', 'vacancy', 'apply'
        ]
        
        try:
            for link in soup.find_all('a', href=True)[:20]:  # Limit to first 20 links
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
        except Exception as e:
            logger.debug(f"Error finding job links: {e}")
        
        return job_links

    def has_job_listings(self, soup: BeautifulSoup) -> bool:
        """Enhanced detection of job listings on page"""
        try:
            # Check for job-specific content
            page_text = soup.get_text().lower()
            job_indicators = ['product manager', 'project manager', 'program manager', 'business analyst', 'strategy manager']
            if any(indicator in page_text for indicator in job_indicators):
                return True
                
            # Check for common job listing structures
            job_indicators = [
                soup.find_all(class_=re.compile(r'job|position|opening|career|role', re.I)),
                soup.find_all('div', {'data-qa': re.compile(r'job|opening', re.I)}),
                soup.find_all('div', {'data-testid': re.compile(r'job|position', re.I)}),
                soup.find_all(['button', 'a'], string=re.compile(r'apply|view job|see details', re.I))
            ]
            
            return any(len(indicators) > 0 for indicators in job_indicators)
        except Exception as e:
            logger.debug(f"Error checking for job listings: {e}")
            return False

    def detect_job_board_type(self, url: str, soup: BeautifulSoup) -> str:
        """Detect which type of job board this is"""
        try:
            # Check URL for known patterns
            for pattern, board_type in self.job_board_patterns.items():
                if pattern in url:
                    return board_type
            
            # Check page content for indicators
            page_content = str(soup).lower()
            if 'greenhouse' in page_content and soup.find(attrs={'data-qa': True}):
                return 'greenhouse'
            elif 'bamboohr' in page_content:
                return 'bamboohr'
            elif soup.find(attrs={'data-automation-id': True}):
                return 'workday'
            elif soup.find(class_=re.compile(r'posting', re.I)):
                return 'lever'
        except Exception as e:
            logger.debug(f"Error detecting job board type: {e}")
        
        return 'generic'

    def extract_jobs_from_page(self, url: str, company: Company) -> List[JobListing]:
        """Extract job listings from a page with smart detection"""
        jobs = []
        session = self.get_session()
        
        try:
            logger.debug(f"Extracting jobs from: {url}")
            response = session.get(url, timeout=15, allow_redirects=True)
            if response.status_code != 200:
                logger.warning(f"Failed to fetch {url}: {response.status_code}")
                return jobs
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Detect job board type
            board_type = self.detect_job_board_type(url, soup)
            logger.debug(f"Detected job board type: {board_type}")
            
            # Use appropriate extraction method
            if board_type in self.job_selectors and board_type != 'generic':
                jobs = self.extract_with_selectors(soup, company, url, board_type)
            else:
                jobs = self.extract_with_generic_method(soup, company, url)
            
            # Filter for target roles
            target_jobs = [job for job in jobs if self.is_target_job_role(job.title)]
            
            if target_jobs:
                logger.info(f"  ✅ Found {len(target_jobs)} target jobs")
            
            return target_jobs
                
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
                try:
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
                    logger.debug(f"Error extracting job from container: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error with {board_type} selectors: {e}")
        
        return jobs

    def extract_with_generic_method(self, soup: BeautifulSoup, company: Company, url: str) -> List[JobListing]:
        """Generic extraction method for unknown job boards"""
        jobs = []
        
        try:
            # Try multiple strategies
            job_elements = []
            
            # Strategy 1: Find elements containing job keywords
            for pattern in self.job_patterns:
                try:
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
                except Exception as e:
                    logger.debug(f"Error with pattern {pattern}: {e}")
                    continue
            
            # Strategy 2: Look for common job container patterns
            for selector in self.job_selectors['generic']['container']:
                try:
                    containers = soup.select(selector)
                    for container in containers:
                        try:
                            if any(pattern.search(container.get_text()) for pattern in self.job_patterns):
                                job_elements.append(container)
                        except Exception as e:
                            logger.debug(f"Error checking container text: {e}")
                            continue
                except Exception as e:
                    logger.debug(f"Error with selector {selector}: {e}")
                    continue
            
            # Extract job details from found elements
            for element in job_elements:
                try:
                    job = self.extract_job_from_element(element, company, url)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.debug(f"Error extracting job from element: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error with generic extraction: {e}")
        
        return jobs

    def extract_job_from_element(self, element, company: Company, base_url: str) -> Optional[JobListing]:
        """Extract job details from a single element with improved title extraction"""
        try:
            # Find title using multiple strategies with better filtering
            title = ""
            for selector in self.job_selectors['generic']['title']:
                try:
                    title_elem = element.select_one(selector)
                    if title_elem:
                        potential_title = title_elem.get_text().strip()
                        # Better title validation
                        if (len(potential_title) > 5 and 
                            len(potential_title) < 100 and  # Not too long
                            not self.is_description_text(potential_title)):  # Not description text
                            title = potential_title
                            break
                except Exception as e:
                    logger.debug(f"Error with title selector {selector}: {e}")
                    continue
            
            if not title:
                # Fallback: use the first reasonable line of text
                try:
                    text_lines = [line.strip() for line in element.get_text().split('\n') if line.strip()]
                    for line in text_lines[:3]:  # Check first 3 lines only
                        if (len(line) > 5 and 
                            len(line) < 100 and 
                            not self.is_description_text(line)):
                            title = line
                            break
                except Exception as e:
                    logger.debug(f"Error extracting fallback title: {e}")
            
            if not title or not self.is_target_job_role(title):
                return None
            
            # Find job URL
            job_url = base_url
            try:
                link = element.find('a', href=True)
                if link:
                    href = link['href']
                    if href.startswith(('http://', 'https://')):
                        job_url = href
                    elif href.startswith('/'):
                        job_url = urljoin(base_url, href)
            except Exception as e:
                logger.debug(f"Error extracting job URL: {e}")
            
            # Find location
            location = ""
            for selector in self.job_selectors['generic']['location']:
                try:
                    loc_elem = element.select_one(selector)
                    if loc_elem:
                        location = loc_elem.get_text().strip()
                        if len(location) < 200:  # Reasonable location length
                            break
                except Exception as e:
                    logger.debug(f"Error with location selector {selector}: {e}")
                    continue
            
            return JobListing(
                title=title,
                company=company.name,
                url=job_url,
                location=location,
                description=element.get_text().strip()[:300] if element else "",
                source="careers"
            )
            
        except Exception as e:
            logger.debug(f"Error extracting job details: {e}")
            return None

    def is_description_text(self, text: str) -> bool:
        """Check if text looks like description/marketing copy rather than a job title"""
        text_lower = text.lower()
        
        # Common description/marketing phrases that indicate this isn't a job title
        description_indicators = [
            'track projects', 'offer', 'thrive', 'deploy', 'manage the lifecycle',
            'we offer', 'our teams', 'you will', 'responsible for', 'work with',
            'looking for', 'join us', 'opportunity to', 'benefits', 'culture',
            'committed to', 'fostered', 'variety of', 'based on', 'allowing',
            'performed remotely', 'collaboration', 'work policy', 'office location',
            'improve my skills', 'expand my field', 'value flexibility', 'hybrid model',
            'spend time', 'promote relationships', 'prioritize employee', 'wellbeing',
            'mental health', 'fully remote', 'accommodating', 'impact you make'
        ]
        
        # If text contains these phrases, it's likely description text, not a title
        if any(indicator in text_lower for indicator in description_indicators):
            return True
            
        # Check for excessive punctuation or weird formatting
        if text.count('.') > 2 or text.count(',') > 3:
            return True
            
        # Check for sentence-like structure (starts with capital, ends with period)
        if len(text) > 50 and text.endswith('.'):
            return True
            
        return False

    def is_target_job_role(self, title: str) -> bool:
        """Check if job title matches any of our target keywords"""
        try:
            return any(pattern.search(title) for pattern in self.job_patterns)
        except Exception as e:
            logger.debug(f"Error checking job role: {e}")
            return False

    def scrape_indeed(self, company: Company) -> List[JobListing]:
        """Scrape Indeed for additional jobs (placeholder)"""
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
        """Scrape all jobs for a single company with improved logic"""
        logger.info(f"📊 Scanning {company.name} ({company.size})...")
        all_jobs = []
        
        try:
            # Step 1: Try company career pages first
            career_urls = self.discover_career_urls(company.careers_url, company.name)
            
            if not career_urls:
                logger.warning(f"No career URLs found for {company.name}")
                career_urls = [company.careers_url]  # Fallback
            
            logger.debug(f"Found {len(career_urls)} career page(s) to check")
            
            # Scrape career pages
            for url in career_urls:
                try:
                    jobs = self.extract_jobs_from_page(url, company)
                    all_jobs.extend(jobs)
                    
                    # Respectful delay
                    time.sleep(random.uniform(1, 2))
                    
                except Exception as e:
                    logger.error(f"Error with {url}: {e}")
            
            # Remove duplicates from company site
            unique_jobs = self.remove_duplicates(all_jobs)
            
            # Step 2: Only use job boards if NO jobs found on company site
            if len(unique_jobs) == 0:
                logger.info(f"  No jobs found on {company.name} career site, checking job boards...")
                
                # Try Indeed first
                try:
                    indeed_jobs = self.scrape_indeed(company)
                    if indeed_jobs:
                        logger.info(f"  ✅ Found {len(indeed_jobs)} jobs on Indeed")
                        unique_jobs.extend(indeed_jobs)
                    else:
                        logger.info(f"  No jobs found on Indeed")
                except Exception as e:
                    logger.error(f"Error scraping Indeed for {company.name}: {e}")
            
            else:
                logger.info(f"  ✅ Found {len(unique_jobs)} jobs on company site, skipping job boards")
            
            # Final deduplication
            final_jobs = self.remove_duplicates(unique_jobs)
            
            total_jobs = len(final_jobs)
            if total_jobs > 0:
                logger.info(f"  ✅ Total: {total_jobs} target jobs")
            else:
                logger.info("  No target jobs found")
            
            return final_jobs
            
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

    def save_results(self, jobs: List[JobListing], filename: str = "target_jobs.csv"):
        """Save jobs to CSV file"""
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as file:
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
            
            logger.info(f"✅ Jobs saved to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")

    def run_scan(self, companies_file: str = "companies_final_ready.csv", max_workers: int = 3):
        """Run the complete scan with optional threading"""
        logger.info("🚀 Starting multi-platform job scan...")
        
        # Load companies and existing jobs
        companies = self.load_companies(companies_file)
        self.load_existing_jobs()
        
        if not companies:
            logger.error("No companies loaded. Exiting.")
            return
        
        logger.info(f"Keywords: {', '.join(self.job_keywords[:10])}...")
        if len(self.job_keywords) > 10:
            logger.info(f"... and {len(self.job_keywords) - 10} more keywords")
        
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
                time.sleep(random.uniform(1, 2))  # Delay between companies
        
        # Final results
        logger.info("\n🎉 SCAN COMPLETE!")
        logger.info(f"Companies scanned: {len(companies)}")
        logger.info(f"Total target jobs found: {len(all_jobs)}")
        
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
        companies_file="companies_final_with_specialization.csv",
        max_workers=3  # Adjust based on your needs
    )
    
    print(f"\nScan complete! Found {len(jobs)} total target jobs.")
