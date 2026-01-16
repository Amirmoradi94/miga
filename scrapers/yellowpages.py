"""Yellow Pages scraper implementation."""
from typing import Dict, List, Optional
from loguru import logger
from sqlalchemy.orm import Session
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, quote_plus
import re
import json
import time
from scrapers.base import BaseScraper
from utils.zyte_client import ZyteClient


class YellowPagesScraper(BaseScraper):
    """Scraper for Yellow Pages business listings."""
    
    def __init__(self, db_session: Session, zyte_api_key: Optional[str] = None):
        super().__init__(db_session, zyte_api_key)
        self.source = "yellowpages"
        self.base_url = "https://www.yellowpages.com"
        self.zyte_client = ZyteClient(self.zyte_api_key) if self.zyte_api_key else None
    
    def build_search_url(self, business_title: str, location: str) -> str:
        """
        Build a Yellow Pages search URL from business title and location.
        
        Args:
            business_title: Business category/title (e.g., "Plumbers", "Electricians")
            location: Location to search (e.g., "Montreal, QC", "New York, NY")
            
        Returns:
            Complete Yellow Pages search URL
            
        Example:
            >>> scraper.build_search_url("Plumbers", "Montreal, QC")
            'https://www.yellowpages.com/search?search_terms=Plumbers&geo_location_terms=Montreal%2C+QC'
        """
        params = {
            'search_terms': business_title,
            'geo_location_terms': location
        }
        query_string = urlencode(params)
        return f"{self.base_url}/search?{query_string}"
    
    def scrape_by_category_and_location(
        self, 
        business_title: str, 
        location: str, 
        max_pages: Optional[int] = None
    ) -> List[Dict]:
        """
        Scrape businesses by business title/category and location.
        This is a convenience method that builds the URL and scrapes.
        
        Args:
            business_title: Business category/title (e.g., "Plumbers", "Electricians")
            location: Location to search (e.g., "Montreal, QC", "New York, NY")
            max_pages: Maximum number of pages to scrape (None for all)
            
        Returns:
            List of business data dictionaries
        """
        search_url = self.build_search_url(business_title, location)
        logger.info(f"Scraping {business_title} in {location}")
        return self.scrape_businesses_from_search(search_url, max_pages=max_pages)
    
    def scrape_multiple_categories(
        self,
        business_titles: List[str],
        location: str,
        max_pages_per_category: Optional[int] = None
    ) -> Dict[str, List[Dict]]:
        """
        Scrape multiple business categories in a given location.
        
        Args:
            business_titles: List of business categories to scrape
            location: Location to search (e.g., "Montreal, QC", "New York, NY")
            max_pages_per_category: Maximum pages to scrape per category (None for all)
            
        Returns:
            Dictionary mapping business titles to lists of business data
        """
        results = {}
        
        for business_title in business_titles:
            logger.info(f"Processing category: {business_title}")
            try:
                businesses = self.scrape_by_category_and_location(
                    business_title, 
                    location, 
                    max_pages=max_pages_per_category
                )
                results[business_title] = businesses
                logger.info(f"Found {len(businesses)} businesses for {business_title}")
                
                # Save businesses to database
                for business_data in businesses:
                    if business_data:
                        self.save_business(business_data)
                self.commit()
                
                # Delay between categories to avoid rate limiting
                if business_title != business_titles[-1]:  # Don't delay after last category
                    time.sleep(self.scraping_delay)
                    
            except Exception as e:
                logger.error(f"Error scraping {business_title}: {e}")
                results[business_title] = []
        
        return results
    
    def _extract_rating_from_text(self, text: str) -> Optional[float]:
        """Extract rating from text like '4.5 stars' or '4.5'."""
        try:
            match = re.search(r'(\d+\.?\d*)\s*(?:star|rating)', text, re.IGNORECASE)
            if match:
                return float(match.group(1))
            # Try to find just a number
            match = re.search(r'(\d+\.\d+)', text)
            if match:
                return float(match.group(1))
        except Exception:
            pass
        return None
    
    def _extract_review_count(self, text: str) -> Optional[int]:
        """Extract review count from text like '(123 reviews)' or '123 reviews'."""
        try:
            match = re.search(r'(\d+)\s*review', text, re.IGNORECASE)
            if match:
                return int(match.group(1))
        except Exception:
            pass
        return None
    
    def _parse_address(self, address_text: str) -> Dict[str, Optional[str]]:
        """
        Parse address text into components.
        
        Args:
            address_text: Full address string
            
        Returns:
            Dictionary with address, city, state, zip_code
        """
        result = {
            'address': None,
            'city': None,
            'state': None,
            'zip_code': None
        }
        
        if not address_text:
            return result
        
        # Try to parse common address formats
        # Format: "123 Main St, City, State ZIP"
        parts = [p.strip() for p in address_text.split(',')]
        
        if len(parts) >= 1:
            result['address'] = parts[0]
        
        if len(parts) >= 2:
            result['city'] = parts[1]
        
        if len(parts) >= 3:
            # Last part might be "State ZIP" or just "State"
            state_zip = parts[2].strip()
            # Try to extract ZIP code
            zip_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', state_zip)
            if zip_match:
                result['zip_code'] = zip_match.group(1)
                result['state'] = state_zip[:zip_match.start()].strip()
            else:
                result['state'] = state_zip
        
        return result
    
    def _parse_business_from_listing(self, listing_element) -> Optional[Dict]:
        """Parse business data from a search result listing element."""
        try:
            business_data = {
                'name': None,
                'source': self.source,
                'source_url': None,
                'source_id': None,
                'phone': None,
                'email': None,
                'website': None,
                'address': None,
                'city': None,
                'state': None,
                'zip_code': None,
                'country': 'USA',
                'latitude': None,
                'longitude': None,
                'category': None,
                'description': None,
                'rating': None,
                'review_count': None,
                'hours': None,
                'amenities': None,
                'images': None,
            }
            
            # Extract business name and URL
            # Yellow Pages typically uses various selectors for business name
            name_selectors = [
                ('a', {'class': re.compile(r'business-name', re.I)}),
                ('h2', {}),
                ('h3', {}),
                ('a', {'class': re.compile(r'listing', re.I)}),
                ('span', {'class': re.compile(r'business-name', re.I)}),
            ]
            
            name_link = None
            for tag, attrs in name_selectors:
                name_link = listing_element.find(tag, attrs)
                if name_link:
                    break
            
            if name_link:
                # Get name
                business_data['name'] = name_link.get_text(strip=True)
                
                # Get URL
                if name_link.name == 'a':
                    href = name_link.get('href', '')
                else:
                    # If not a link, look for parent link
                    parent_link = name_link.find_parent('a')
                    href = parent_link.get('href', '') if parent_link else ''
                
                if href:
                    if href.startswith('/'):
                        business_data['source_url'] = urljoin(self.base_url, href.split('?')[0])
                    elif href.startswith('http'):
                        business_data['source_url'] = href.split('?')[0]
                    else:
                        business_data['source_url'] = urljoin(self.base_url, href.split('?')[0])
                    
                    # Extract source_id from URL
                    match = re.search(r'/([^/]+)\.html', business_data['source_url'])
                    if match:
                        business_data['source_id'] = match.group(1)
            
            # Extract phone number
            phone_selectors = [
                ('div', {'class': re.compile(r'phone', re.I)}),
                ('span', {'class': re.compile(r'phone', re.I)}),
                ('a', {'href': re.compile(r'tel:', re.I)}),
                ('div', {'itemprop': 'telephone'}),
            ]
            
            for tag, attrs in phone_selectors:
                phone_elem = listing_element.find(tag, attrs)
                if phone_elem:
                    phone_text = phone_elem.get_text(strip=True)
                    # Extract phone number pattern
                    phone_match = re.search(r'[\d\s\-\(\)\.]+', phone_text)
                    if phone_match:
                        business_data['phone'] = re.sub(r'\s+', ' ', phone_match.group(0)).strip()
                        break
            
            # Extract address
            address_selectors = [
                ('div', {'class': re.compile(r'address', re.I)}),
                ('span', {'class': re.compile(r'address', re.I)}),
                ('div', {'itemprop': 'address'}),
                ('div', {'class': re.compile(r'location', re.I)}),
            ]
            
            for tag, attrs in address_selectors:
                address_elem = listing_element.find(tag, attrs)
                if address_elem:
                    address_text = address_elem.get_text(strip=True)
                    if address_text:
                        address_parts = self._parse_address(address_text)
                        business_data.update(address_parts)
                        break
            
            # Extract website
            website_elem = listing_element.find('a', href=re.compile(r'^https?://', re.I))
            if website_elem:
                href = website_elem.get('href', '')
                # Exclude Yellow Pages internal links
                if 'yellowpages.com' not in href.lower():
                    business_data['website'] = href
            
            # Extract rating
            rating_selectors = [
                ('div', {'class': re.compile(r'rating', re.I)}),
                ('span', {'class': re.compile(r'rating', re.I)}),
                ('div', {'itemprop': 'ratingValue'}),
            ]
            
            for tag, attrs in rating_selectors:
                rating_elem = listing_element.find(tag, attrs)
                if rating_elem:
                    rating_text = rating_elem.get_text(strip=True)
                    business_data['rating'] = self._extract_rating_from_text(rating_text)
                    if business_data['rating']:
                        break
            
            # Extract review count
            review_selectors = [
                ('span', {'class': re.compile(r'review', re.I)}),
                ('div', {'class': re.compile(r'review', re.I)}),
            ]
            
            for tag, attrs in review_selectors:
                review_elem = listing_element.find(tag, attrs)
                if review_elem:
                    review_text = review_elem.get_text(strip=True)
                    business_data['review_count'] = self._extract_review_count(review_text)
                    if business_data['review_count']:
                        break
            
            # Extract categories
            category_selectors = [
                ('div', {'class': re.compile(r'category', re.I)}),
                ('span', {'class': re.compile(r'category', re.I)}),
                ('a', {'class': re.compile(r'category', re.I)}),
            ]
            
            categories = []
            for tag, attrs in category_selectors:
                category_elems = listing_element.find_all(tag, attrs)
                for elem in category_elems:
                    cat_text = elem.get_text(strip=True)
                    if cat_text and cat_text not in categories:
                        categories.append(cat_text)
            
            if categories:
                business_data['category'] = ', '.join(categories[:5])  # Limit to 5 categories
            
            # Extract description
            desc_elem = listing_element.find('div', class_=re.compile(r'description|snippet', re.I))
            if desc_elem:
                business_data['description'] = desc_elem.get_text(strip=True)
            
            # Extract image
            img_elem = listing_element.find('img')
            if img_elem:
                img_url = img_elem.get('src', '') or img_elem.get('data-src', '')
                if img_url:
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        img_url = urljoin(self.base_url, img_url)
                    business_data['images'] = json.dumps([img_url])
            
            return business_data if business_data['name'] and business_data['source_url'] else None
            
        except Exception as e:
            logger.error(f"Error parsing business listing: {e}")
            return None
    
    def scrape_businesses_from_search(self, search_url: str, max_pages: Optional[int] = None) -> List[Dict]:
        """
        Scrape businesses directly from search results.
        
        Args:
            search_url: URL of the Yellow Pages search results page
            max_pages: Maximum number of pages to scrape (None for all)
            
        Returns:
            List of business data dictionaries
        """
        logger.info(f"Scraping businesses from Yellow Pages search results: {search_url}")
        
        businesses = []
        page = 1
        
        try:
            while True:
                # Build URL with pagination
                if page > 1:
                    parsed_url = urlparse(search_url)
                    query_params = parse_qs(parsed_url.query)
                    query_params['page'] = [str(page)]
                    new_query = '&'.join([f"{k}={v[0]}" for k, v in query_params.items()])
                    current_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"
                else:
                    current_url = search_url
                
                logger.info(f"Fetching page {page}: {current_url}")
                
                # Fetch page using Zyte API
                if not self.zyte_client:
                    logger.error("Zyte API client not initialized")
                    break
                
                response = self.zyte_client.fetch_page(current_url)
                if not response or 'browserHtml' not in response:
                    logger.warning(f"No response for page {page}")
                    break
                
                html = response['browserHtml']
                soup = self.zyte_client.parse_html(html)
                if not soup:
                    logger.warning(f"Failed to parse HTML for page {page}")
                    break
                
                # Find business listings - Yellow Pages uses various structures
                listings = []
                
                # Try multiple selectors for business listings
                listing_selectors = [
                    ('div', {'class': re.compile(r'result', re.I)}),
                    ('div', {'class': re.compile(r'listing', re.I)}),
                    ('div', {'class': re.compile(r'business', re.I)}),
                    ('article', {}),
                    ('li', {'class': re.compile(r'result', re.I)}),
                ]
                
                for tag, attrs in listing_selectors:
                    found_listings = soup.find_all(tag, attrs)
                    if found_listings:
                        listings = found_listings
                        logger.debug(f"Found {len(listings)} listings using selector: {tag} {attrs}")
                        break
                
                if not listings:
                    logger.warning(f"No business listings found on page {page}")
                    break
                
                page_businesses = []
                for listing in listings:
                    business_data = self._parse_business_from_listing(listing)
                    if business_data:
                        page_businesses.append(business_data)
                        businesses.append(business_data)
                
                logger.info(f"Found {len(page_businesses)} businesses on page {page}")
                
                if not page_businesses:
                    logger.info("No businesses found on this page, stopping")
                    break
                
                # Check for next page
                next_page_selectors = [
                    ('a', {'class': re.compile(r'next', re.I)}),
                    ('a', {'aria-label': re.compile(r'next', re.I)}),
                    ('a', {'title': re.compile(r'next', re.I)}),
                ]
                
                has_next_page = False
                for tag, attrs in next_page_selectors:
                    next_link = soup.find(tag, attrs)
                    if next_link and next_link.get('href'):
                        has_next_page = True
                        break
                
                if not has_next_page:
                    logger.info("No next page found")
                    break
                
                page += 1
                
                if max_pages and page > max_pages:
                    logger.info(f"Reached max pages limit: {max_pages}")
                    break
                
                # Delay between requests
                time.sleep(self.scraping_delay)
            
            logger.info(f"Total businesses scraped: {len(businesses)}")
            return businesses
            
        except Exception as e:
            logger.error(f"Error scraping businesses from search results {search_url}: {e}")
            return businesses
    
    def scrape_search_results(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """
        Scrape Yellow Pages search results to get business URLs.
        
        Args:
            search_url: URL of the Yellow Pages search results page
            max_pages: Maximum number of pages to scrape (None for all)
            
        Returns:
            List of business page URLs
        """
        logger.info(f"Scraping Yellow Pages search results: {search_url}")
        
        business_urls = []
        page = 1
        
        try:
            while True:
                # Build URL with pagination
                if page > 1:
                    parsed_url = urlparse(search_url)
                    query_params = parse_qs(parsed_url.query)
                    query_params['page'] = [str(page)]
                    new_query = '&'.join([f"{k}={v[0]}" for k, v in query_params.items()])
                    current_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"
                else:
                    current_url = search_url
                
                logger.info(f"Fetching page {page}: {current_url}")
                
                # Fetch page using Zyte API
                if not self.zyte_client:
                    logger.error("Zyte API client not initialized")
                    break
                
                response = self.zyte_client.fetch_page(current_url)
                if not response or 'browserHtml' not in response:
                    logger.warning(f"No response for page {page}")
                    break
                
                html = response['browserHtml']
                soup = self.zyte_client.parse_html(html)
                if not soup:
                    logger.warning(f"Failed to parse HTML for page {page}")
                    break
                
                # Find business listings
                listings = []
                listing_selectors = [
                    ('div', {'class': re.compile(r'result', re.I)}),
                    ('div', {'class': re.compile(r'listing', re.I)}),
                    ('div', {'class': re.compile(r'business', re.I)}),
                    ('article', {}),
                ]
                
                for tag, attrs in listing_selectors:
                    found_listings = soup.find_all(tag, attrs)
                    if found_listings:
                        listings = found_listings
                        break
                
                if not listings:
                    logger.warning(f"No business listings found on page {page}")
                    break
                
                page_business_urls = []
                for listing in listings:
                    # Find business name link
                    name_selectors = [
                        ('a', {'class': re.compile(r'business-name', re.I)}),
                        ('h2', {}),
                        ('h3', {}),
                        ('a', {'class': re.compile(r'listing', re.I)}),
                    ]
                    
                    name_link = None
                    for tag, attrs in name_selectors:
                        name_link = listing.find(tag, attrs)
                        if name_link:
                            break
                    
                    if name_link:
                        if name_link.name == 'a':
                            href = name_link.get('href', '')
                        else:
                            parent_link = name_link.find_parent('a')
                            href = parent_link.get('href', '') if parent_link else ''
                        
                        if href and '/biz/' in href or '.html' in href:
                            if href.startswith('/'):
                                full_url = urljoin(self.base_url, href.split('?')[0])
                            elif href.startswith('http'):
                                full_url = href.split('?')[0]
                            else:
                                full_url = urljoin(self.base_url, href.split('?')[0])
                            
                            if full_url not in business_urls:
                                business_urls.append(full_url)
                                page_business_urls.append(full_url)
                
                logger.info(f"Found {len(page_business_urls)} businesses on page {page}")
                
                if not page_business_urls:
                    logger.info("No businesses found on this page, stopping")
                    break
                
                # Check for next page
                next_page_selectors = [
                    ('a', {'class': re.compile(r'next', re.I)}),
                    ('a', {'aria-label': re.compile(r'next', re.I)}),
                ]
                
                has_next_page = False
                for tag, attrs in next_page_selectors:
                    next_link = soup.find(tag, attrs)
                    if next_link and next_link.get('href'):
                        has_next_page = True
                        break
                
                if not has_next_page:
                    logger.info("No next page found")
                    break
                
                page += 1
                
                if max_pages and page > max_pages:
                    logger.info(f"Reached max pages limit: {max_pages}")
                    break
                
                # Delay between requests
                time.sleep(self.scraping_delay)
            
            logger.info(f"Total business URLs found: {len(business_urls)}")
            return business_urls
            
        except Exception as e:
            logger.error(f"Error scraping Yellow Pages search results {search_url}: {e}")
            return business_urls
    
    def scrape_business(self, url: str) -> Optional[Dict]:
        """
        Scrape a single Yellow Pages business page.
        
        Args:
            url: URL of the Yellow Pages business page
            
        Returns:
            Dictionary containing business data or None if failed
        """
        logger.info(f"Scraping Yellow Pages business: {url}")
        
        try:
            if not self.zyte_client:
                logger.error("Zyte API client not initialized")
                return None
            
            # Fetch page using Zyte API
            response = self.zyte_client.fetch_page(url)
            if not response or 'browserHtml' not in response:
                logger.error(f"No response for business page: {url}")
                return None
            
            html = response['browserHtml']
            soup = self.zyte_client.parse_html(html)
            if not soup:
                logger.error(f"Failed to parse HTML for business page: {url}")
                return None
            
            business_data = {
                'name': None,
                'source': self.source,
                'source_url': url,
                'source_id': None,
                'phone': None,
                'email': None,
                'website': None,
                'address': None,
                'city': None,
                'state': None,
                'zip_code': None,
                'country': 'USA',
                'latitude': None,
                'longitude': None,
                'category': None,
                'description': None,
                'rating': None,
                'review_count': None,
                'hours': None,
                'amenities': None,
                'images': None,
            }
            
            # Extract source_id from URL
            match = re.search(r'/([^/]+)\.html', url)
            if match:
                business_data['source_id'] = match.group(1)
            
            # Extract business name
            name_selectors = [
                ('h1', {}),
                ('h2', {'class': re.compile(r'business-name', re.I)}),
                ('div', {'class': re.compile(r'business-name', re.I)}),
            ]
            
            for tag, attrs in name_selectors:
                name_elem = soup.find(tag, attrs)
                if name_elem:
                    business_data['name'] = name_elem.get_text(strip=True)
                    break
            
            # Extract phone
            phone_selectors = [
                ('div', {'class': re.compile(r'phone', re.I)}),
                ('a', {'href': re.compile(r'tel:', re.I)}),
                ('span', {'itemprop': 'telephone'}),
            ]
            
            for tag, attrs in phone_selectors:
                phone_elem = soup.find(tag, attrs)
                if phone_elem:
                    phone_text = phone_elem.get_text(strip=True)
                    phone_match = re.search(r'[\d\s\-\(\)\.]+', phone_text)
                    if phone_match:
                        business_data['phone'] = re.sub(r'\s+', ' ', phone_match.group(0)).strip()
                        break
            
            # Extract address
            address_selectors = [
                ('div', {'class': re.compile(r'address', re.I)}),
                ('span', {'itemprop': 'address'}),
                ('div', {'itemprop': 'address'}),
            ]
            
            for tag, attrs in address_selectors:
                address_elem = soup.find(tag, attrs)
                if address_elem:
                    address_text = address_elem.get_text(strip=True)
                    if address_text:
                        address_parts = self._parse_address(address_text)
                        business_data.update(address_parts)
                        break
            
            # Extract website
            website_elem = soup.find('a', href=re.compile(r'^https?://', re.I))
            if website_elem and 'yellowpages.com' not in website_elem.get('href', '').lower():
                business_data['website'] = website_elem.get('href', '').strip()
            
            # Extract rating
            rating_elem = soup.find('div', class_=re.compile(r'rating', re.I))
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                business_data['rating'] = self._extract_rating_from_text(rating_text)
            
            # Extract review count
            review_elem = soup.find('span', class_=re.compile(r'review', re.I))
            if review_elem:
                review_text = review_elem.get_text(strip=True)
                business_data['review_count'] = self._extract_review_count(review_text)
            
            # Extract categories
            category_elems = soup.find_all('a', href=re.compile(r'/search\?search_terms='))
            categories = []
            for elem in category_elems[:5]:
                cat_text = elem.get_text(strip=True)
                if cat_text and cat_text not in categories:
                    categories.append(cat_text)
            if categories:
                business_data['category'] = ', '.join(categories)
            
            # Extract description
            desc_elem = soup.find('div', class_=re.compile(r'description|about', re.I))
            if desc_elem:
                business_data['description'] = desc_elem.get_text(strip=True)
            
            # Extract hours
            hours_elem = soup.find('div', class_=re.compile(r'hours|schedule', re.I))
            if hours_elem:
                business_data['hours'] = hours_elem.get_text(strip=True)
            
            # Extract images
            img_elems = soup.find_all('img', src=re.compile(r'\.(jpg|jpeg|png)', re.I))
            images = []
            for img in img_elems[:5]:  # Limit to 5 images
                img_url = img.get('src', '') or img.get('data-src', '')
                if img_url:
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        img_url = urljoin(self.base_url, img_url)
                    images.append(img_url)
            if images:
                business_data['images'] = json.dumps(images)
            
            return business_data if business_data['name'] else None
            
        except Exception as e:
            logger.error(f"Error scraping Yellow Pages business {url}: {e}")
            return None
