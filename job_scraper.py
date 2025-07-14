import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime
import os
from notion_client import Client

def load_companies(csv_file):
    """Load companies from CSV file"""
    try:
        df = pd.read_csv(csv_file)
        return df.to_dict('records')
    except Exception as e:
        print(f"Error loading companies: {e}")
        return []

def load_keywords(config_file):
    """Load global keywords from config file"""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config.get('keywords', [])
    except Exception as e:
        print(f"Error loading keywords: {e}")
        return ["product manager", "software engineer", "data analyst"]  # Default keywords

def scrape_job_page(url, keywords):
    """Scrape a single job page for keywords"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        page_text = soup.get_text().lower()
        
        # Find job listings (common selectors)
        job_elements = soup.find_all(['div', 'article', 'section'], 
                                   class_=re.compile(r'job|position|career|opening', re.I))
        
        jobs_found = []
        
        # If no specific job elements found, search entire page
        if not job_elements:
            job_elements = [soup]
        
        for element in job_elements:
            element_text = element.get_text().lower()
            
            # Check if any keywords match
            found_keywords = []
            for keyword in keywords:
                if keyword.lower() in element_text:
                    found_keywords.append(keyword)
            
            if found_keywords:
                # Try to extract job title and link
                title_element = element.find(['h1', 'h2', 'h3', 'h4', 'a'])
                title = title_element.get_text().strip() if title_element else "Job Opening"
                
                # Try to find job link
                link_element = element.find('a', href=True)
                job_url = link_element['href'] if link_element else url
                
                # Make relative URLs absolute
                if job_url.startswith('/'):
                    base_url = '/'.join(url.split('/')[:3])
                    job_url = base_url + job_url
                
                jobs_found.append({
                    'title': title[:100],  # Limit length
                    'url': job_url,
                    'keywords_found': found_keywords,
                    'description': element_text[:200]  # First 200 chars
                })
        
        return jobs_found
        
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return []

def send_to_notion(jobs, notion_token, database_id):
    """Send jobs to Notion database"""
    if not notion_token or not database_id:
        print("Notion credentials not provided, skipping Notion sync")
        return
    
    try:
        notion = Client(auth=notion_token)
        
        for job in jobs:
            # Create new page in Notion database
            new_page = {
                "parent": {"database_id": database_id},
                "properties": {
                    "Job Title": {"title": [{"text": {"content": job['title']}}]},
                    "Company": {"rich_text": [{"text": {"content": job['company']}}]},
                    "Description": {"rich_text": [{"text": {"content": job['description']}}]},
                    "URL": {"url": job['url']},
                    "Keywords Found": {"multi_select": [{"name": kw} for kw in job['keywords_found']]},
                    "Date Found": {"date": {"start": datetime.now().isoformat()}},
                    "Status": {"select": {"name": "New"}},
                    "Source": {"select": {"name": "Daily Scan"}}
                }
            }
            
            notion.pages.create(**new_page)
            print(f"Added to Notion: {job['title']} at {job['company']}")
            
    except Exception as e:
        print(f"Error sending to Notion: {e}")

def main():
    # Load configuration
    companies = load_companies('companies.csv')
    global_keywords = load_keywords('config.json')
    
    # Get environment variables
    notion_token = os.getenv('NOTION_TOKEN')
    notion_database_id = os.getenv('NOTION_DATABASE_ID')
    
    all_jobs = []
    
    print(f"Starting scan of {len(companies)} companies...")
    print(f"Using keywords: {', '.join(global_keywords)}")
    
    for company in companies:
        if company.get('Industry', '').lower() == 'other':
            continue  # Skip companies categorized as 'Other'
            
        print(f"Scanning {company['Company']}...")
        
        # Use global keywords for all companies
        jobs = scrape_job_page(company['Careers Site URL'], global_keywords)
        
        # Add company info to each job
        for job in jobs:
            job['company'] = company['Company']
            job['industry'] = company.get('Industry', 'Unknown')
            job['location'] = company.get('City', 'Unknown')
        
        all_jobs.extend(jobs)
        
        # Be polite - wait between requests
        time.sleep(2)
    
    print(f"Found {len(all_jobs)} job opportunities")
    
    # Save results to JSON file
    results = {
        'scan_date': datetime.now().isoformat(),
        'total_jobs': len(all_jobs),
        'jobs': all_jobs
    }
    
    with open('results/latest_scan.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Send to Notion if configured
    if all_jobs:
        send_to_notion(all_jobs, notion_token, notion_database_id)
    
    print("Scan complete!")

if __name__ == "__main__":
    main()
