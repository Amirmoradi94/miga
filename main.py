"""Main entry point for the scraper application."""
import sys
from loguru import logger
from database import engine, get_db, Base
from scrapers.yelp import YelpScraper
from scrapers.yellowpages import YellowPagesScraper
from config import settings

# Configure logger
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    level="INFO"
)


def init_database():
    """Initialize database tables."""
    logger.info("Initializing database...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database initialized successfully")


def main():
    """Main function to run scrapers."""
    logger.info("Starting Miga scraper application...")
    
    # Initialize database
    init_database()
    
    # Get database session
    db = next(get_db())
    
    try:
        # Initialize scrapers
        yelp_scraper = YelpScraper(db, settings.zyte_api_key)
        yellowpages_scraper = YellowPagesScraper(db, settings.zyte_api_key)
        
        # Example 1: Scrape a single category by title and location
        # businesses = yelp_scraper.scrape_by_category_and_location(
        #     business_title="Plumbers",
        #     location="Montreal",
        #     max_pages=3
        # )
        # for business_data in businesses:
        #     if business_data:
        #         yelp_scraper.save_business(business_data)
        # yelp_scraper.commit()
        
        # Example 2: Scrape multiple categories at once
        # business_categories = [
        #     "Plumbers",
        #     "Electricians",
        #     "Contractors & Handymen",
        #     "Venues & Events",
        #     "Auto Repair",
        #     "Dentists",
        #     # Add more categories as needed
        # ]
        # results = yelp_scraper.scrape_multiple_categories(
        #     business_titles=business_categories,
        #     location="Montreal",
        #     max_pages_per_category=2
        # )
        # # Results are automatically saved to database
        # for category, businesses in results.items():
        #     logger.info(f"{category}: {len(businesses)} businesses found")
        
        # Example 3: Build URL manually and scrape
        # search_url = yelp_scraper.build_search_url("Venues & Events", "Montreal")
        # businesses = yelp_scraper.scrape_businesses_from_search(search_url, max_pages=3)
        # for business_data in businesses:
        #     if business_data:
        #         yelp_scraper.save_business(business_data)
        # yelp_scraper.commit()
        
        logger.info("Scraping completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
