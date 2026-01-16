"""Zyte API client utility."""
from typing import Optional, Dict, Any
from loguru import logger
import json
import time
import requests
from bs4 import BeautifulSoup


class ZyteClient:
    """Client for interacting with Zyte API."""
    
    def __init__(self, api_key: str):
        """
        Initialize Zyte API client.
        
        Args:
            api_key: Zyte API key
        """
        self.api_key = api_key
        self.base_url = "https://api.zyte.com/v1/extract"
    
    def fetch_page(self, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Fetch a page using Zyte API.
        
        Args:
            url: URL to fetch
            **kwargs: Additional parameters for the API request (e.g., browserHtml=True)
            
        Returns:
            Dictionary containing page data (browserHtml, etc.) or None if failed
        """
        logger.info(f"Fetching page with Zyte API: {url}")
        
        try:
            # Zyte API request payload
            # Default to browserHtml=True if not specified
            payload = {
                "url": url,
                "browserHtml": kwargs.get("browserHtml", True),
                **{k: v for k, v in kwargs.items() if k != "browserHtml"}
            }
            
            # Zyte API uses HTTP Basic Auth with API key as username and empty password
            response = requests.post(
                self.base_url,
                auth=(self.api_key, ""),
                json=payload,
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                return result
            else:
                logger.error(f"Zyte API error {response.status_code}: {response.text}")
                return None
            
        except Exception as e:
            logger.error(f"Error fetching page {url} with Zyte API: {e}")
            return None
    
    def parse_html(self, html: str) -> Optional[BeautifulSoup]:
        """
        Parse HTML content.
        
        Args:
            html: HTML content string
            
        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            return BeautifulSoup(html, 'lxml')
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return None
