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

class AbsenceSoftDebugScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    
    def load_keywords(self):
        """Load keywords with debug info"""
        try:
            with open('config_fixed.json', 'r') as f:
                config = json.load(f)
            keywords = config.get('keywords', [])
            print(f"✅ Loaded {len(keywords)} keywords from config_fixed.json")
            print(f"First 10 keywords: {keywords[:10]}")
            return keywords
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            # Hardcoded keywords for debugging
            keywords = [
                "product manager", "Product Manager", 
                "project manager", "Project Manager",
                "product management", "Product Management",
                "project management", "Project Management"
            ]
            print(f"✅ Using hardcoded keywords: {keywords}")
            return keywords
    
    def debug_absencesoft_page(self, url, keywords):
        """Debug what we find on AbsenceSoft careers page"""
        print(f"\n🔍 DEBUGGING ABSENCESOFT PAGE: {url}")
        
        try:
            response = self.session.get(url, timeout=15)
            print(f"Status code: {response.status_code}")
            
            if response.status_code != 200:
                print(f"❌ HTTP Error: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = soup.get_text()
            
            print(f"Page length: {len(page_text)} characters")
            print(f"Page title: {soup.title.string if soup.title else 'No title'}")
            
            # Look for manager mentions
            manager_lines = []
            for line in page_text.split('\n'):
                line = line.strip()
                if 'manager' in line.lower() and len(line) > 5:
                    manager_lines.append(line)
            
            print(f"\n📋 Found {len(manager_lines)} lines containing 'manager':")
            for i, line in enumerate(manager_lines[:10]):  # Show first 10
                print(f"  {i+1}. {line}")
            
            # Check for specific keywords
            print(f"\n🔍 Keyword analysis:")
            for keyword in keywords[:8]:  # Check first 8 keywords
                if keyword.lower() in page_text.lower():
                    print(f"  ✅ Found: '{keyword}'")
                else:
                    print(f"  ❌ Missing: '{keyword}'")
            
            # Look for job elements
            job_elements = soup.find_all(['div', 'article', 'section', 'li'], 
                                       class_=re.compile(r'job|position|career|opening|listing', re.I))
            
            print(f"\n🏢 Found {len(job_elements)} potential job elements")
            
            if not job_elements:
                print("No specific job elements found, scanning entire page")
                job_elements = [soup]
            
            jobs_found = []
            
            for i, element in enumerate(job_elements[:10]):  # Debug first 10
                element_text = element.get_text()
                print(f"\n--- Job Element {i+1} ---")
                print(f"Element text preview: {element_text[:200]}...")
                
                # Check for keyword matches
                found_keywords = []
                for keyword in keywords:
                    if keyword.lower() in element_text.lower():
                        found_keywords.append(keyword)
                
                print(f"Keywords found in element: {found_keywords}")
                
                # Categorize keywords
                core_keywords = [kw for kw in found_keywords if kw.lower() not in ['remote', 'hybrid']]
                print(f"Core keywords: {core_keywords}")
                
                if core_keywords:
                    # Try to extract title
                    title_strategies = [
                        lambda: element.find(['h1', 'h2', 'h3'], class_=re.compile(r'job.*title|title.*job', re.I)),
                        lambda: element.find('a', string=re.compile(r'manager', re.I)),
                        lambda: element.find(['h1', 'h2', 'h3', 'h4', 'h5']),
                        lambda: element.find(['strong', 'b'])
                    ]
                    
                    title = "No title found"
                    for strategy in title_strategies:
                        try:
                            title_element = strategy()
                            if title_element and len(title_element.get_text().strip()) > 3:
                                title = title_element.get_text().strip()
                                title = re.sub(r'\s+', ' ', title)
                                break
                        except:
                            continue
                    
                    print(f"Extracted title: '{title}'")
                    
                    # Extract URL
                    job_url = url
                    link_element = element.find('a', href=True)
                    if link_element:
                        href = link_element['href']
                        if href.startswith('/'):
                            job_url = '/'.join(url.split('/')[:3]) + href
                        elif href.startswith('http'):
                            job_url = href
                        print(f"Job URL: {job_url}")
                    
                    if len(title) > 5 and title.lower() not in ['no title found', 'learn more', 'apply']:
                        jobs_found.append({
                            'title': title,
                            'url': job_url,
                            'keywords_found': core_keywords,
                            'source': 'Careers Page'
                        })
                        print(f"✅ JOB FOUND: {title}")
                    else:
                        print(f"❌ Skipped (bad title): {title}")
                else:
                    print(f"❌ Skipped (no core keywords)")
            
            print(f"\n🎯 FINAL RESULT: Found {len(jobs_found)} jobs")
            for job in jobs_found:
                print(f"  - {job['title']} (Keywords: {job['keywords_found']})")
            
            return jobs_found
            
        except Exception as e:
            print(f"❌ Error debugging page: {e}")
            return []
    
    def run_debug_scan(self):
        """Run debug scan on just AbsenceSoft"""
        print("🚀 ABSENCESOFT DEBUG SCAN STARTING")
        
        # Load keywords
        keywords = self.load_keywords()
        
        # AbsenceSoft URL - UPDATE THIS LINE WITH THE CORRECT URL
        absencesoft_url = "https://absencesoft.com/careers/#openings"  # ← Updated with correct URL
        
        print(f"\n🎯 Target URL: {absencesoft_url}")
        
        # Debug the page
        jobs = self.debug_absencesoft_page(absencesoft_url, keywords)
        
        print(f"\n📊 SCAN COMPLETE")
        print(f"Jobs found: {len(jobs)}")
        
        # Try to add to Notion if configured
        notion_token = os.getenv('NOTION_TOKEN')
        notion_database_id = os.getenv('NOTION_DATABASE_ID')
        
        if notion_token and notion_database_id and jobs:
            print(f"\n📝 Attempting to add to Notion...")
            notion = Client(auth=notion_token)
            
            for job in jobs:
                try:
                    new_page = {
                        "parent": {"database_id": notion_database_id},
                        "properties": {
                            "Job Title": {"title": [{"text": {"content": job['title']}}]},
                            "Company": {"rich_text": [{"text": {"content": "AbsenceSoft"}}]},
                            "Industry": {"select": {"name": "Software & Services"}},
                            "Job URL": {"url": job['url']},
                            "Source": {"select": {"name": job['source']}},
                            "Keywords Found": {"multi_select": [{"name": kw} for kw in job['keywords_found']]},
                            "Date Found": {"date": {"start": datetime.now().strftime('%Y-%m-%d')}},
                            "Status": {"select": {"name": "New"}},
                            "Unique ID": {"rich_text": [{"text": {"content": f"debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}}]}
                        }
                    }
                    
                    notion.pages.create(**new_page)
                    print(f"✅ Added to Notion: {job['title']}")
                except Exception as e:
                    print(f"❌ Failed to add to Notion: {e}")
        
        return jobs

if __name__ == "__main__":
    scraper = AbsenceSoftDebugScraper()
    scraper.run_debug_scan()
