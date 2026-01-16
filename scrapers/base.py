"""Base scraper class with common functionality."""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from loguru import logger
from sqlalchemy.orm import Session
from models import Business
from config import settings


class BaseScraper(ABC):
    """Base class for all scrapers."""
    
    def __init__(self, db_session: Session, zyte_api_key: Optional[str] = None):
        """
        Initialize the scraper.
        
        Args:
            db_session: Database session
            zyte_api_key: Zyte API key for web scraping
        """
        self.db = db_session
        self.zyte_api_key = zyte_api_key or settings.zyte_api_key
        self.scraping_delay = settings.scraping_delay
        self.max_retries = settings.max_retries
    
    @abstractmethod
    def scrape_business(self, url: str) -> Optional[Dict]:
        """
        Scrape a single business page.
        
        Args:
            url: URL of the business page
            
        Returns:
            Dictionary containing business data or None if failed
        """
        pass
    
    @abstractmethod
    def scrape_search_results(self, search_url: str) -> List[str]:
        """
        Scrape search results to get business URLs.
        
        Args:
            search_url: URL of the search results page
            
        Returns:
            List of business page URLs
        """
        pass
    
    def save_business(self, business_data: Dict) -> Optional[Business]:
        """
        Save business data to database.
        
        Args:
            business_data: Dictionary containing business information
            
        Returns:
            Business model instance or None if failed
        """
        try:
            # Check if business already exists
            existing = self.db.query(Business).filter_by(
                source_url=business_data.get('source_url')
            ).first()
            
            if existing:
                # Update existing business
                for key, value in business_data.items():
                    if hasattr(existing, key) and value is not None:
                        setattr(existing, key, value)
                logger.info(f"Updated business: {existing.name}")
                return existing
            else:
                # Create new business
                business = Business(**business_data)
                self.db.add(business)
                logger.info(f"Created new business: {business.name}")
                return business
                
        except Exception as e:
            logger.error(f"Error saving business: {e}")
            self.db.rollback()
            return None
    
    def commit(self):
        """Commit database changes."""
        try:
            self.db.commit()
            logger.info("Database changes committed")
        except Exception as e:
            logger.error(f"Error committing to database: {e}")
            self.db.rollback()
            raise
