"""Database models for business data."""
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Boolean
from sqlalchemy.sql import func
from database import Base


class Business(Base):
    """Model for storing business information."""
    
    __tablename__ = "businesses"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Basic Information
    name = Column(String(255), nullable=False, index=True)
    source = Column(String(50), nullable=False, index=True)  # 'yelp' or 'yellowpages'
    source_url = Column(String(500), unique=True, nullable=False)
    source_id = Column(String(100), index=True)  # ID from source website
    
    # Contact Information
    phone = Column(String(50))
    email = Column(String(255))
    website = Column(String(500))
    
    # Location
    address = Column(Text)
    city = Column(String(100), index=True)
    state = Column(String(50), index=True)
    zip_code = Column(String(20))
    country = Column(String(100), default="USA")
    latitude = Column(Float)
    longitude = Column(Float)
    
    # Business Details
    category = Column(String(255))
    description = Column(Text)
    rating = Column(Float)
    review_count = Column(Integer)
    
    # Additional Information (stored as JSON string or separate fields)
    hours = Column(Text)  # JSON string for business hours
    amenities = Column(Text)  # JSON string for amenities/features
    images = Column(Text)  # JSON string for image URLs
    
    # Metadata
    scraped_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    
    def __repr__(self):
        return f"<Business(name='{self.name}', source='{self.source}')>"
