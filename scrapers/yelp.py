"""Yelp scraper implementation."""
from typing import Dict, List, Optional
from loguru import logger
from sqlalchemy.orm import Session
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, quote_plus
import re
import json
import time
from scrapers.base import BaseScraper
from utils.zyte_client import ZyteClient


class YelpScraper(BaseScraper):
    """Scraper for Yelp business listings."""
    
    def __init__(self, db_session: Session, zyte_api_key: Optional[str] = None):
        super().__init__(db_session, zyte_api_key)
        self.source = "yelp"
        self.base_url = "https://www.yelp.ca"
        self.zyte_client = ZyteClient(self.zyte_api_key) if self.zyte_api_key else None
    
    def build_search_url(self, business_title: str, location: str) -> str:
        """
        Build a Yelp search URL from business title and location.
        
        Args:
            business_title: Business category/title (e.g., "Plumbers", "Venues & Events")
            location: Location to search (e.g., "Montreal", "Toronto, ON")
            
        Returns:
            Complete Yelp search URL
            
        Example:
            >>> scraper.build_search_url("Plumbers", "Montreal")
            'https://www.yelp.ca/search?find_desc=Plumbers&find_loc=Montreal'
        """
        params = {
            'find_desc': business_title,
            'find_loc': location
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
            business_title: Business category/title (e.g., "Plumbers", "Venues & Events")
            location: Location to search (e.g., "Montreal", "Toronto, ON")
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
            location: Location to search (e.g., "Montreal", "Toronto, ON")
            max_pages_per_category: Maximum pages to scrape per category (None for all)
            
        Returns:
            Dictionary mapping business titles to lists of business data
            
        Example:
            >>> categories = ["Plumbers", "Electricians", "Contractors"]
            >>> results = scraper.scrape_multiple_categories(categories, "Montreal", max_pages_per_category=2)
            >>> print(f"Found {len(results['Plumbers'])} plumbers")
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
    
    def _extract_rating_from_aria_label(self, element) -> Optional[float]:
        """Extract rating from aria-label attribute."""
        try:
            aria_label = element.get('aria-label', '')
            match = re.search(r'(\d+\.?\d*)\s*star', aria_label, re.IGNORECASE)
            if match:
                return float(match.group(1))
        except Exception:
            pass
        return None
    
    def _extract_review_count(self, text: str) -> Optional[int]:
        """Extract review count from text like '(1 review)' or '(9 reviews)'."""
        try:
            match = re.search(r'\((\d+)\s*review', text, re.IGNORECASE)
            if match:
                return int(match.group(1))
        except Exception:
            pass
        return None
    
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
                'country': 'Canada',
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
            name_link = listing_element.find('h3', class_='y-css-hcgwj4')
            if name_link:
                link = name_link.find('a', class_='y-css-12f4fi2')
                if link:
                    business_data['name'] = link.get_text(strip=True)
                    href = link.get('href', '')
                    if href:
                        if href.startswith('/'):
                            business_data['source_url'] = urljoin(self.base_url, href.split('?')[0])
                        else:
                            business_data['source_url'] = href.split('?')[0]
                        
                        # Extract source_id from URL (e.g., /biz/swift-home-services-c%C3%B4te-saint-luc-2)
                        match = re.search(r'/biz/([^/?]+)', business_data['source_url'])
                        if match:
                            business_data['source_id'] = match.group(1)
            
            # Extract rating
            rating_elem = listing_element.find('div', class_='y-css-dnttlc', role='img')
            if rating_elem:
                business_data['rating'] = self._extract_rating_from_aria_label(rating_elem)
            
            # Extract review count
            rating_text_elem = listing_element.find('div', attrs={'data-traffic-crawl-id': 'SearchResultBizRating'})
            if rating_text_elem:
                review_text = rating_text_elem.get_text(strip=True)
                business_data['review_count'] = self._extract_review_count(review_text)
            
            # Extract categories
            categories_elem = listing_element.find('div', attrs={'data-testid': 'serp-ia-categories'})
            if categories_elem:
                category_buttons = categories_elem.find_all('button', class_='y-css-4nc3wq')
                categories = [btn.get_text(strip=True) for btn in category_buttons]
                if categories:
                    business_data['category'] = ', '.join(categories)
            
            # Extract address
            address_elem = listing_element.find('address')
            if address_elem:
                address_p = address_elem.find('p', class_='y-css-194gzdn')
                if address_p:
                    address_span = address_p.find('span', class_='raw__09f24__T4Ezm')
                    if address_span:
                        business_data['address'] = address_span.get_text(strip=True)
            
            # Extract city/area
            secondary_attrs = listing_element.find('div', class_='secondaryAttributes__09f24__F0z3u')
            if secondary_attrs:
                container = secondary_attrs.find('div', class_='container__09f24__Ommk4')
                if container:
                    # Try to find city in paragraph
                    city_p = container.find('p', class_='y-css-194gzdn')
                    if city_p:
                        city_text = city_p.get_text(strip=True)
                        # Handle "Serving X and the Surrounding Area" or just city name
                        if 'Serving' in city_text:
                            match = re.search(r'Serving\s+([^,]+)', city_text)
                            if match:
                                business_data['city'] = match.group(1).strip()
                        else:
                            business_data['city'] = city_text
                    
                    # Try to find address in address tag
                    address_tag = container.find('address')
                    if address_tag:
                        address_p = address_tag.find('p', class_='y-css-194gzdn')
                        if address_p:
                            address_span = address_p.find('span', class_='raw__09f24__T4Ezm')
                            if address_span:
                                business_data['address'] = address_span.get_text(strip=True)
                    
                    # Try to find city in div after address
                    city_div = container.find('div', class_='y-css-74ugvt')
                    if city_div:
                        city_p = city_div.find('p', class_='y-css-194gzdn')
                        if city_p:
                            business_data['city'] = city_p.get_text(strip=True)
            
            # Extract amenities/tags
            tags = []
            tag_elements = listing_element.find_all('div', class_='tag__09f24__wuJ8a', attrs={'data-testid': 'tag'})
            for tag_elem in tag_elements:
                tag_text_elem = tag_elem.find('span', class_='tagText__09f24__OoFU9')
                if tag_text_elem:
                    tag_span = tag_text_elem.find('span', class_='raw__09f24__T4Ezm')
                    if tag_span:
                        tags.append(tag_span.get_text(strip=True))
            
            if tags:
                business_data['amenities'] = json.dumps(tags)
            
            # Extract image URL
            img_elem = listing_element.find('img', class_='y-css-fex5b')
            if img_elem:
                img_url = img_elem.get('src', '')
                if img_url:
                    business_data['images'] = json.dumps([img_url])
            
            return business_data if business_data['name'] and business_data['source_url'] else None
            
        except Exception as e:
            logger.error(f"Error parsing business listing: {e}")
            return None
    
    def scrape_businesses_from_search(self, search_url: str, max_pages: Optional[int] = None) -> List[Dict]:
        """
        Scrape businesses directly from search results (more efficient than visiting each page).
        
        Args:
            search_url: URL of the Yelp search results page
            max_pages: Maximum number of pages to scrape (None for all)
            
        Returns:
            List of business data dictionaries
        """
        logger.info(f"Scraping businesses from Yelp search results: {search_url}")
        
        businesses = []
        page = 0
        start = 0
        
        try:
            while True:
                # Build URL with pagination
                if page > 0:
                    parsed_url = urlparse(search_url)
                    query_params = parse_qs(parsed_url.query)
                    query_params['start'] = [str(start)]
                    new_query = '&'.join([f"{k}={v[0]}" for k, v in query_params.items()])
                    current_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"
                else:
                    current_url = search_url
                
                logger.info(f"Fetching page {page + 1}: {current_url}")
                
                # Fetch page using Zyte API
                if not self.zyte_client:
                    logger.error("Zyte API client not initialized")
                    break
                
                response = self.zyte_client.fetch_page(current_url)
                if not response or 'browserHtml' not in response:
                    logger.warning(f"No response for page {page + 1}")
                    break
                
                html = response['browserHtml']
                soup = self.zyte_client.parse_html(html)
                if not soup:
                    logger.warning(f"Failed to parse HTML for page {page + 1}")
                    break
                
                # Find main container
                main = soup.find('main', id='main-content', class_='searchResultsContainer__09f24__jckwW')
                if not main:
                    logger.warning(f"Main container not found on page {page + 1}")
                    break
                
                # Find all business listings
                listings = main.find_all('li', class_='y-css-mhg9c5')
                page_businesses = []
                
                for listing in listings:
                    # Check if this is a business listing (has business name)
                    name_link = listing.find('h3', class_='y-css-hcgwj4')
                    if name_link:
                        business_data = self._parse_business_from_listing(listing)
                        if business_data:
                            page_businesses.append(business_data)
                            businesses.append(business_data)
                
                logger.info(f"Found {len(page_businesses)} businesses on page {page + 1}")
                
                # Check if there are more pages
                pagination = main.find('div', class_='pagination__09f24__D23mv')
                if not pagination or not page_businesses:
                    logger.info("No more pages or no businesses found")
                    break
                
                # Check for next page link
                next_link = pagination.find('a', class_='next-link')
                if not next_link:
                    logger.info("No next page link found")
                    break
                
                page += 1
                start += 10
                
                if max_pages and page >= max_pages:
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
        Scrape Yelp search results to get business URLs.
        
        Args:
            search_url: URL of the Yelp search results page
            max_pages: Maximum number of pages to scrape (None for all)
            
        Returns:
            List of business page URLs
        """
        logger.info(f"Scraping Yelp search results: {search_url}")
        
        business_urls = []
        page = 0
        start = 0
        
        try:
            while True:
                # Build URL with pagination
                if page > 0:
                    parsed_url = urlparse(search_url)
                    query_params = parse_qs(parsed_url.query)
                    query_params['start'] = [str(start)]
                    new_query = '&'.join([f"{k}={v[0]}" for k, v in query_params.items()])
                    current_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"
                else:
                    current_url = search_url
                
                logger.info(f"Fetching page {page + 1}: {current_url}")
                
                # Fetch page using Zyte API
                if not self.zyte_client:
                    logger.error("Zyte API client not initialized")
                    break
                
                response = self.zyte_client.fetch_page(current_url)
                if not response or 'browserHtml' not in response:
                    logger.warning(f"No response for page {page + 1}")
                    break
                
                html = response['browserHtml']
                soup = self.zyte_client.parse_html(html)
                if not soup:
                    logger.warning(f"Failed to parse HTML for page {page + 1}")
                    break
                
                # Find main container
                main = soup.find('main', id='main-content', class_='searchResultsContainer__09f24__jckwW')
                if not main:
                    logger.warning(f"Main container not found on page {page + 1}")
                    break
                
                # Find all business listings
                listings = main.find_all('li', class_='y-css-mhg9c5')
                page_business_urls = []
                
                for listing in listings:
                    # Check if this is a business listing (has business name)
                    name_link = listing.find('h3', class_='y-css-hcgwj4')
                    if name_link:
                        link = name_link.find('a', class_='y-css-12f4fi2')
                        if link:
                            href = link.get('href', '')
                            if href and '/biz/' in href:
                                if href.startswith('/'):
                                    full_url = urljoin(self.base_url, href.split('?')[0])
                                else:
                                    full_url = href.split('?')[0]
                                
                                if full_url not in business_urls:
                                    business_urls.append(full_url)
                                    page_business_urls.append(full_url)
                
                logger.info(f"Found {len(page_business_urls)} businesses on page {page + 1}")
                
                # Check if there are more pages
                pagination = main.find('div', class_='pagination__09f24__D23mv')
                if not pagination or not page_business_urls:
                    logger.info("No more pages or no businesses found")
                    break
                
                # Check for next page link
                next_link = pagination.find('a', class_='next-link')
                if not next_link:
                    logger.info("No next page link found")
                    break
                
                page += 1
                start += 10
                
                if max_pages and page >= max_pages:
                    logger.info(f"Reached max pages limit: {max_pages}")
                    break
                
                # Delay between requests
                time.sleep(self.scraping_delay)
            
            logger.info(f"Total business URLs found: {len(business_urls)}")
            return business_urls
            
        except Exception as e:
            logger.error(f"Error scraping Yelp search results {search_url}: {e}")
            return business_urls
    
    def scrape_business_from_listing(self, listing_element) -> Optional[Dict]:
        """
        Scrape business data directly from a search result listing.
        This is more efficient than visiting each business page.
        
        Args:
            listing_element: BeautifulSoup element of a business listing
            
        Returns:
            Dictionary containing business data or None if failed
        """
        return self._parse_business_from_listing(listing_element)
    
    def scrape_business(self, url: str) -> Optional[Dict]:
        """
        Scrape a single Yelp business page.
        
        Args:
            url: URL of the Yelp business page
            
        Returns:
            Dictionary containing business data or None if failed
        """
        logger.info(f"Scraping Yelp business: {url}")
        
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
                'country': 'Canada',
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
            match = re.search(r'/biz/([^/?]+)', url)
            if match:
                business_data['source_id'] = match.group(1)
            
            # Extract business name (usually in h1)
            name_elem = soup.find('h1')
            if name_elem:
                business_data['name'] = name_elem.get_text(strip=True)
            
            # Extract rating and review count
            rating_elem = soup.find('div', attrs={'data-testid': 'rating'})
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                if rating_match:
                    business_data['rating'] = float(rating_match.group(1))
                
                review_match = re.search(r'(\d+)\s*review', rating_text, re.IGNORECASE)
                if review_match:
                    business_data['review_count'] = int(review_match.group(1))
            
            # Extract address
            address_elem = soup.find('address')
            if address_elem:
                address_parts = [p.get_text(strip=True) for p in address_elem.find_all('p')]
                if address_parts:
                    business_data['address'] = ', '.join(address_parts)
            
            # Extract phone
            phone_elem = soup.find('p', class_=re.compile('phone', re.I))
            if phone_elem:
                phone_text = phone_elem.get_text(strip=True)
                phone_match = re.search(r'[\d\s\-\(\)]+', phone_text)
                if phone_match:
                    business_data['phone'] = phone_match.group(0).strip()
            
            # Extract website
            website_elem = soup.find('a', href=re.compile(r'^https?://', re.I))
            if website_elem and 'biz' not in website_elem.get('href', ''):
                business_data['website'] = website_elem.get('href', '').strip()
            
            # Extract categories
            category_elems = soup.find_all('a', href=re.compile(r'/search\?find_desc='))
            categories = []
            for elem in category_elems[:5]:  # Limit to first 5
                cat_text = elem.get_text(strip=True)
                if cat_text:
                    categories.append(cat_text)
            if categories:
                business_data['category'] = ', '.join(categories)
            
            return business_data if business_data['name'] else None
            
        except Exception as e:
            logger.error(f"Error scraping Yelp business {url}: {e}")
            return None
