#!/usr/bin/env python3
"""
Enhanced Web Crawler Backend - Streamlit-free version
A powerful web crawler that searches for GoWithGuide and affiliate references
"""

import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup, FeatureNotFound
from urllib.parse import urljoin, urlparse, parse_qs, unquote, quote, urlunparse
from collections import deque
import csv
import datetime
import re
import time
from io import StringIO
import json
import hashlib
import html
import logging
import tldextract
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import List, Dict, Optional, Callable


class WebCrawlerAPI:
    """
    Enhanced Web Crawler API - Backend service for crawling websites
    """
    
    def __init__(self, start_url: str, crawl_mode: str = "Standard"):
        self.session = self._create_session()
        self.start_url = start_url
        self.keywords = ["gowithguide", "go with guide", "go-with-guide", "87121"]
        self.main_domain = urlparse(start_url).netloc
        self.crawl_mode = crawl_mode
        self.max_pages = {"Quick": 5, "Standard": 150, "Complete": 5000}[crawl_mode]
        self.visited = set()
        self.results = []
        self.queue = deque([start_url])
        self.categories = []
        self.current_category = None
        self.status_messages = []
        self.user_stopped = False
        self.pages_crawled = 0
        self.redirect_cache = {}
        self.internal_links = set()
        self.crawled_pages_content = {}
        self.url_fragments_checked = set()
        self.is_running = False
        self.progress_callback = None
        self.status_callback = None
        self.results_callback = None
        
        # URL shortener detection
        self.known_shorteners = [
            'bit.ly', 'tinyurl.com', 'goo.gl', 't.co', 'ow.ly', 'is.gd', 
            'buff.ly', 'adf.ly', 'bit.do', 'mcaf.ee', 'su.pr', 'tiny.cc',
            'tidd.ly', 'redirectingat.com', 'go.redirectingat.com', 'go.skimresources.com'
        ]
        self.awin_domains = ['awin1.com', 'zenaps.com']
        self.potential_affiliate_domains = [
            'track.', 'go.', 'click.', 'buy.', 'shop.', 'link.', 'visit.', 
            'affiliate.', 'partners.', 'tracking.', 'redirect.', 'ref.'
        ]
        
        self.setup_logger()

    def setup_logger(self):
        """Setup logging configuration"""
        self.logger = logging.getLogger('crawler')
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _create_session(self):
        """Create HTTP session with retry strategy"""
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })
        return session

    def set_callbacks(self, progress_callback=None, status_callback=None, results_callback=None):
        """Set callback functions for real-time updates"""
        self.progress_callback = progress_callback
        self.status_callback = status_callback
        self.results_callback = results_callback

    def _emit_progress(self, current: int, total: int):
        """Emit progress update"""
        if self.progress_callback:
            self.progress_callback(current, total)

    def _emit_status(self, message: str):
        """Emit status update"""
        self.status_messages.append(message)
        if self.status_callback:
            self.status_callback(message)

    def _emit_results(self):
        """Emit results update"""
        if self.results_callback:
            self.results_callback(self.results)

    def get_soup(self, html_content):
        """Parse HTML content with BeautifulSoup"""
        try:
            return BeautifulSoup(html_content, 'lxml')
        except FeatureNotFound:
            self._emit_status("Warning: 'lxml' parser not found. Using 'html.parser' instead.")
            return BeautifulSoup(html_content, 'html.parser')

    def is_same_domain(self, url):
        """Check if URL belongs to the same domain"""
        main_domain_parts = tldextract.extract(self.start_url)
        url_domain_parts = tldextract.extract(url)
        return (main_domain_parts.domain == url_domain_parts.domain and 
                main_domain_parts.suffix == url_domain_parts.suffix)

    def is_subdomain_of(self, url_netloc):
        """Check if URL is a subdomain of the main domain"""
        main_domain = self.main_domain.replace("www.", "").lower()
        url_netloc = url_netloc.replace("www.", "").lower()
        return url_netloc.endswith("." + main_domain) or url_netloc == main_domain

    def is_relevant_path(self, url):
        """Determine if a URL path is relevant for crawling"""
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        
        irrelevant_patterns = [
            r'/wp-admin', r'/admin', r'/login', r'/register', r'/cart', r'/checkout',
            r'/search', r'/tag/', r'/author/', r'/feed', r'/rss', r'/xml',
            r'\.(css|js|jpg|jpeg|png|gif|pdf|zip|doc|docx|xls|xlsx)$'
        ]
        
        for pattern in irrelevant_patterns:
            if re.search(pattern, path):
                return False
        
        return True

    def normalize_url(self, url):
        """Normalize URL for comparison"""
        parsed = urlparse(url.lower())
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip('/'),
            '',
            '',
            ''
        ))
        return normalized

    def get_matched_keywords(self, text):
        """Find matching keywords in text"""
        if not text:
            return []
        
        text_lower = text.lower()
        matched = []
        
        for keyword in self.keywords:
            if keyword.lower() in text_lower:
                matched.append(keyword)
        
        return matched

    def is_url_shortener(self, domain):
        """Check if domain is a known URL shortener"""
        return any(shortener in domain.lower() for shortener in self.known_shorteners)

    def is_potential_affiliate(self, url):
        """Check if URL might be an affiliate link"""
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Check for known affiliate domains
        if any(awin_domain in domain for awin_domain in self.awin_domains):
            return True
        
        # Check for potential affiliate subdomains
        if any(domain.startswith(prefix) for prefix in self.potential_affiliate_domains):
            return True
        
        # Check for affiliate parameters
        query_params = parse_qs(parsed.query)
        affiliate_params = ['ref', 'aff', 'affiliate', 'partner', 'source', 'utm_source']
        if any(param in query_params for param in affiliate_params):
            return True
        
        return False

    def resolve_redirects(self, url):
        """Resolve URL redirects"""
        if url in self.redirect_cache:
            return self.redirect_cache[url]
        
        try:
            response = self.session.head(url, allow_redirects=True, timeout=10)
            final_url = response.url
            self.redirect_cache[url] = final_url
            return final_url
        except Exception:
            self.redirect_cache[url] = url
            return url

    def check_url_for_keywords(self, url, source_url):
        """Check if URL contains keywords or is an affiliate link"""
        if not url:
            return
        
        # Check URL itself for keywords
        matched_keywords = self.get_matched_keywords(url)
        if matched_keywords:
            self.add_result(
                source_url=source_url,
                matched_url=url,
                element='url',
                attribute='href',
                content=url,
                keywords=matched_keywords,
                location_type='direct_url'
            )
        
        # Check for affiliate indicators
        if self.is_potential_affiliate(url):
            # Resolve redirects for shortened URLs
            if self.is_url_shortener(urlparse(url).netloc):
                final_url = self.resolve_redirects(url)
                final_keywords = self.get_matched_keywords(final_url)
                if final_keywords:
                    self.add_result(
                        source_url=source_url,
                        matched_url=final_url,
                        element='url',
                        attribute='href',
                        content=f"Redirected from: {url}",
                        keywords=final_keywords,
                        location_type='redirect_url'
                    )

    def extract_and_analyze_elements(self, soup, source_url):
        """Extract and analyze page elements for keywords"""
        internal_urls = []
        
        # Check all links
        for link in soup.find_all('a', href=True):
            href = link['href'].strip()
            if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                continue
            
            full_url = urljoin(source_url, href)
            self.check_url_for_keywords(full_url, source_url)
            
            # Collect internal links
            if self.is_subdomain_of(urlparse(full_url).netloc):
                internal_urls.append(full_url)
            
            # Check link text
            link_text = link.get_text().strip()
            matched_kws = self.get_matched_keywords(link_text)
            if matched_kws:
                self.add_result(
                    source_url=source_url,
                    matched_url=full_url,
                    element='a',
                    attribute='text',
                    content=link_text,
                    keywords=matched_kws,
                    location_type='link_text'
                )
        
        # Check images
        for img in soup.find_all('img'):
            for attr in ['src', 'data-src', 'data-lazy-src']:
                if attr in img.attrs:
                    img_url = urljoin(source_url, img[attr])
                    self.check_url_for_keywords(img_url, source_url)
            
            # Check alt text
            alt_text = img.get('alt', '').strip()
            matched_kws = self.get_matched_keywords(alt_text)
            if matched_kws:
                self.add_result(
                    source_url=source_url,
                    matched_url=source_url,
                    element='img',
                    attribute='alt',
                    content=alt_text,
                    keywords=matched_kws,
                    location_type='image_alt'
                )
        
        # Check scripts for URLs
        for script in soup.find_all('script'):
            script_content = script.get_text()
            if script_content:
                urls = re.findall(r'https?://[^\s"\'>]+', script_content)
                for url in urls:
                    self.check_url_for_keywords(url, source_url)
        
        # Check buttons and their text
        for button in soup.find_all(['button', 'input']):
            button_text = ''
            if button.name == 'input' and button.get('type') in ['button', 'submit']:
                button_text = button.get('value', '')
            else:
                button_text = button.get_text().strip()
            
            matched_kws = self.get_matched_keywords(button_text)
            if matched_kws:
                self.add_result(
                    source_url=source_url,
                    matched_url=source_url,
                    element=button.name,
                    attribute='text',
                    content=button_text,
                    keywords=matched_kws,
                    location_type='button_text'
                )
        
        # Check data attributes
        for element in soup.find_all(attrs=lambda x: x and any(attr.startswith('data-') for attr in x)):
            element_type = element.name
            for attr in element.attrs:
                if attr.startswith('data-') and isinstance(element[attr], str):
                    attr_value = element[attr].strip()
                    if ('url' in attr.lower() or 'href' in attr.lower() or 'link' in attr.lower()):
                        if attr_value and attr_value.startswith(('http', '//')):
                            data_url = urljoin(source_url, attr_value)
                            self.check_url_for_keywords(data_url, source_url)
                            if self.is_subdomain_of(urlparse(data_url).netloc):
                                internal_urls.append(data_url)
                    
                    matched_kws = self.get_matched_keywords(attr_value)
                    if matched_kws:
                        self.add_result(
                            source_url=source_url,
                            matched_url=source_url,
                            element=element_type,
                            attribute=attr,
                            content=attr_value,
                            keywords=matched_kws,
                            location_type='data_attribute'
                        )
        
        return internal_urls

    def add_result(self, source_url, matched_url, element, attribute, content, keywords, location_type):
        """Add a match result"""
        for keyword in keywords:
            match_id = hashlib.md5(f"{matched_url}:{element}:{attribute}:{keyword}:{location_type}".encode()).hexdigest()
            if any(result.get('match_id') == match_id for result in self.results):
                continue
            
            result = {
                'source_url': source_url,
                'matched_url': matched_url,
                'element': element,
                'attribute': attribute,
                'keyword': keyword,
                'content': content[:500] if content else '',
                'location_type': location_type,
                'timestamp': datetime.datetime.now().isoformat(),
                'match_id': match_id
            }
            self.results.append(result)
            self.logger.info(f"Found affiliate link: {matched_url} (keyword: {keyword})")
            self._emit_results()

    def process_url(self, url):
        """Process a single URL and extract information"""
        if url in self.visited or self.user_stopped:
            return []
        
        self.visited.add(url)
        self.pages_crawled += 1
        self._emit_status(f"Processing: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            if 'text/html' not in response.headers.get('content-type', ''):
                return []
            
            soup = self.get_soup(response.text)
            internal_urls = self.extract_and_analyze_elements(soup, url)
            
            # Store page content for potential analysis
            self.crawled_pages_content[url] = {
                'title': soup.title.string if soup.title else '',
                'content': soup.get_text()[:1000],  # First 1000 chars
                'links_found': len(internal_urls)
            }
            
            return internal_urls
            
        except Exception as e:
            self._emit_status(f"Error processing {url}: {str(e)}")
            return []

    def extract_categories(self):
        """Extract categories from the main page"""
        try:
            response = self.session.get(self.start_url)
            soup = self.get_soup(response.text)
            categories = []
            
            category_priority = [
                'travel', 'blog', 'resources', 'guides', 'destinations',
                'tours', 'activities', 'affiliate', 'partner', 'reviews',
                'articles', 'news', 'features', 'stories'
            ]
            
            nav_elements = soup.find_all(
                ['nav', 'ul', 'div'],
                class_=lambda c: c and any(term in c.lower() for term in ['nav', 'menu', 'categories', 'main-menu'])
            )
            
            for nav in nav_elements:
                for link in nav.find_all('a', href=True):
                    href = link['href'].lower()
                    text = link.get_text().lower().strip()
                    
                    if not href or href.startswith('#'):
                        continue
                    if any(term in href for term in ['/cart', '/login', '/register', '/account']):
                        continue
                    
                    full_url = urljoin(self.start_url, href)
                    if not self.is_same_domain(full_url):
                        continue
                    
                    cat_name = None
                    cat_match = re.search(r'/(?:category|topics|sections?|tags?)/([^/]+)', href)
                    if cat_match:
                        cat_name = cat_match.group(1).lower()
                    else:
                        for cat in category_priority:
                            if cat in href or cat in text:
                                cat_name = cat
                                break
                    
                    if not cat_name:
                        if 3 <= len(text) <= 20 and ' ' not in text:
                            cat_name = text
                        else:
                            path_parts = urlparse(full_url).path.strip('/').split('/')
                            if path_parts:
                                cat_name = path_parts[-1].lower()
                            else:
                                cat_name = 'other'
                    
                    categories.append((cat_name, full_url))
            
            # Sort categories by priority
            sorted_categories = []
            for cat in category_priority:
                matched = [c for c in categories if c[0] == cat]
                if matched:
                    sorted_categories.append(matched[0])
            
            remaining = [c for c in categories if c[0] not in [sc[0] for sc in sorted_categories]]
            sorted_categories.extend(remaining)
            
            # Remove duplicates
            unique_cat_urls = set()
            unique_categories = []
            for cat_name, cat_url in sorted_categories:
                if cat_url not in unique_cat_urls:
                    unique_cat_urls.add(cat_url)
                    unique_categories.append((cat_name, cat_url))
                    if len(unique_categories) >= 10:
                        break
            
            return unique_categories
            
        except Exception as e:
            self._emit_status(f"Error extracting categories: {str(e)}")
            return []

    def get_main_pages(self):
        """Get main pages from the website"""
        try:
            response = self.session.get(self.start_url)
            soup = self.get_soup(response.text)
            main_links = []
            seen_urls = set()
            
            content_patterns = [
                r'/(?:post|article|blog|story|guide)s?/',
                r'/(?:travel|destination|tour)s?/',
                r'/(?:partner|affiliate)s?/',
                r'/(?:review|resource)s?/'
            ]
            
            priority_terms = [
                'affiliate', 'partner', 'sponsor', 'advertis', 
                'review', 'guid', 'tour', 'travel', 'destination', 'blog'
            ]
            
            all_links = []
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                    continue
                
                url = urljoin(self.start_url, href)
                if not self.is_same_domain(url):
                    continue
                
                if re.search(r'\.(jpg|jpeg|png|gif|pdf|zip|doc|docx)$', url, re.I):
                    continue
                
                text = link.get_text().strip().lower()
                priority = 0
                
                for pattern in content_patterns:
                    if re.search(pattern, url, re.I):
                        priority += 3
                
                for term in priority_terms:
                    if term in url.lower() or term in text:
                        priority += 2
                
                parent = link.find_parent(['article', 'section', 'main'])
                if parent:
                    priority += 1
                
                all_links.append((url, priority))
            
            all_links.sort(key=lambda x: x[1], reverse=True)
            
            for url, _ in all_links:
                normalized_url = self.normalize_url(url)
                if normalized_url not in seen_urls and self.is_relevant_path(normalized_url):
                    seen_urls.add(normalized_url)
                    main_links.append(url)
                    if len(main_links) >= self.max_pages:
                        break
            
            return main_links
            
        except Exception as e:
            self._emit_status(f"Error getting main pages: {str(e)}")
            return []

    def get_category_pages(self, category_url):
        """Get pages from a specific category"""
        try:
            response = self.session.get(category_url)
            soup = self.get_soup(response.text)
            article_links = []
            seen_urls = set()
            
            article_elements = soup.find_all(
                ['article', 'div', 'section', 'li'],
                class_=lambda c: c and any(term in (c.lower() if c else '') for term in ['post', 'article', 'entry', 'item', 'card', 'stories'])
            )
            
            if article_elements:
                for container in article_elements:
                    links = container.find_all('a', href=True)
                    for link in links:
                        url = urljoin(category_url, link['href'].strip())
                        if not self.is_same_domain(url) or not self.is_relevant_path(url):
                            continue
                        
                        normalized_url = self.normalize_url(url)
                        if normalized_url not in seen_urls:
                            seen_urls.add(normalized_url)
                            article_links.append(url)
            
            # Fallback: search for article-like URLs
            if len(article_links) < self.max_pages:
                for link in soup.find_all('a', href=True):
                    url = urljoin(category_url, link['href'].strip())
                    normalized_url = self.normalize_url(url)
                    
                    if normalized_url in seen_urls:
                        continue
                    if not self.is_same_domain(url) or not self.is_relevant_path(url):
                        continue
                    
                    if (re.search(r'/(?:article|post|blog|news|story|guide)/', url) or
                        re.search(r'/\d{4}/\d{2}/', url)):
                        seen_urls.add(normalized_url)
                        article_links.append(url)
                        if len(article_links) >= self.max_pages:
                            break
            
            return article_links[:self.max_pages]
            
        except Exception as e:
            self._emit_status(f"Error getting category pages: {str(e)}")
            return []

    def stop_crawling(self):
        """Stop the crawling process"""
        self.user_stopped = True
        self.is_running = False
        self._emit_status("Crawling stopped by user")

    def reset(self):
        """Reset the crawler state"""
        self.visited.clear()
        self.results.clear()
        self.queue.clear()
        self.queue.append(self.start_url)
        self.categories.clear()
        self.status_messages.clear()
        self.pages_crawled = 0
        self.user_stopped = False
        self.is_running = False
        self.crawled_pages_content.clear()

    async def crawl_async(self):
        """Asynchronous crawling method"""
        self.is_running = True
        self.user_stopped = False
        
        try:
            if self.crawl_mode == "Quick":
                await self._crawl_quick()
            elif self.crawl_mode == "Standard":
                await self._crawl_standard()
            elif self.crawl_mode == "Complete":
                await self._crawl_complete()
        finally:
            self.is_running = False

    async def _crawl_quick(self):
        """Quick crawl mode - just the homepage"""
        urls_to_crawl = [self.start_url]
        for i, url in enumerate(urls_to_crawl):
            if self.user_stopped:
                break
            self.process_url(url)
            self._emit_progress(i + 1, len(urls_to_crawl))

    async def _crawl_standard(self):
        """Standard crawl mode - homepage and main pages"""
        if not self.categories:
            homepage_links = self.get_main_pages()
            self._emit_status(f"Crawling homepage and {len(homepage_links)} main pages...")
            urls_to_crawl = [self.start_url] + homepage_links
            
            for i, url in enumerate(urls_to_crawl[:self.max_pages]):
                if self.user_stopped:
                    break
                self.process_url(url)
                self._emit_progress(i + 1, min(self.max_pages, len(urls_to_crawl)))
                
                if self.results:
                    self._emit_status(f"Found {len(self.results)} matches")
                    break
            
            if not self.results:
                self.categories = self.extract_categories()
                if self.categories:
                    self._emit_status(f"Found categories: {', '.join([c[0] for c in self.categories])}")
                else:
                    self._emit_status("No categories found.")
                    return
        
        # Process categories
        while self.categories and not self.user_stopped:
            cat_name, cat_url = self.categories.pop(0)
            self._emit_status(f"Processing category: {cat_name}")
            category_links = self.get_category_pages(cat_url)
            
            for i, url in enumerate(category_links[:self.max_pages]):
                if self.user_stopped:
                    break
                self.process_url(url)
                self._emit_progress(i + 1, min(self.max_pages, len(category_links)))
                
                if self.results:
                    self._emit_status(f"Found {len(self.results)} matches")
                    break

    async def _crawl_complete(self):
        """Complete crawl mode - thorough crawling"""
        while self.queue and self.pages_crawled < self.max_pages and not self.user_stopped:
            url = self.queue.popleft()
            if url not in self.visited:
                self._emit_status(f"Crawling: {url} (Page {self.pages_crawled + 1}/{self.max_pages})")
                new_urls = self.process_url(url)
                
                for new_url in new_urls:
                    if (new_url not in self.visited and 
                        new_url not in self.queue and 
                        self.pages_crawled < self.max_pages):
                        self.queue.append(new_url)
                
                self._emit_progress(self.pages_crawled, self.max_pages)
                
                if self.results:
                    self._emit_status(f"Found {len(self.results)} matches")

    def generate_csv_data(self):
        """Generate CSV data from results"""
        csv_file = StringIO()
        writer = csv.DictWriter(csv_file, fieldnames=[
            'source_url', 'matched_url', 'keyword', 
            'location_type', 'element', 'attribute',
            'content_sample', 'timestamp'
        ])
        writer.writeheader()
        
        for result in self.results:
            writer.writerow({
                'source_url': result['source_url'],
                'matched_url': result['matched_url'],
                'keyword': result['keyword'],
                'location_type': result['location_type'],
                'element': result['element'],
                'attribute': result['attribute'],
                'content_sample': result['content'][:300] if result['content'] else '',
                'timestamp': result['timestamp']
            })
        
        return csv_file.getvalue()

    def get_status(self):
        """Get current crawler status"""
        return {
            'is_running': self.is_running,
            'pages_crawled': self.pages_crawled,
            'max_pages': self.max_pages,
            'results_count': len(self.results),
            'categories_remaining': len(self.categories),
            'status_messages': self.status_messages[-10:],  # Last 10 messages
            'mode': self.crawl_mode
        }

    def get_results(self):
        """Get current results"""
        return {
            'results': self.results,
            'count': len(self.results),
            'csv_data': self.generate_csv_data() if self.results else None
        }


if __name__ == "__main__":
    # Example usage
    import asyncio
    
    def on_progress(current, total):
        print(f"Progress: {current}/{total} ({current/total*100:.1f}%)")
    
    def on_status(message):
        print(f"Status: {message}")
    
    def on_results(results):
        print(f"Results updated: {len(results)} matches found")
    
    async def main():
        crawler = WebCrawlerAPI("https://example.com", "Quick")
        crawler.set_callbacks(on_progress, on_status, on_results)
        
        await crawler.crawl_async()
        
        status = crawler.get_status()
        results = crawler.get_results()
        
        print(f"Final status: {status}")
        print(f"Final results: {results['count']} matches")

    # asyncio.run(main())
