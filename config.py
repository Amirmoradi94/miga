"""Configuration settings for the scraper application."""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database
    database_url: str = "postgresql://miga_user:miga_password@localhost:5432/miga_db"
    
    # Zyte API
    zyte_api_key: Optional[str] = None
    
    # Scraping settings
    scraping_delay: int = 1
    max_retries: int = 3
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
