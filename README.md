# Miga - Business Scraper

A dockerized application for scraping local business information from Yelp and Yellow Pages websites and storing them in a PostgreSQL database.

## Features

- Scrapes business data from Yelp and Yellow Pages
- Uses Zyte API for web scraping
- Stores data in PostgreSQL database
- Dockerized for easy deployment
- Extensible scraper architecture

## Project Structure

```
miga/
├── scrapers/
│   ├── __init__.py
│   ├── base.py          # Base scraper class
│   ├── yelp.py          # Yelp scraper implementation
│   └── yellowpages.py   # Yellow Pages scraper implementation
├── config.py            # Configuration settings
├── database.py          # Database connection and session
├── models.py            # Database models
├── main.py              # Main entry point
├── requirements.txt     # Python dependencies
├── Dockerfile           # Docker image definition
├── docker-compose.yml   # Docker Compose configuration
└── README.md           # This file
```

## Setup

1. **Clone the repository** (if applicable)

2. **Set up environment variables**
   
   A `.env` file has been created in the project root. Edit it and add your credentials:
   
   ```bash
   # Required: Add your Zyte API key
   ZYTE_API_KEY=your_actual_zyte_api_key_here
   
   # Optional: Update database credentials if needed
   POSTGRES_USER=miga_user
   POSTGRES_PASSWORD=miga_password
   POSTGRES_DB=miga_db
   ```
   
   **Important:** 
   - Get your Zyte API key from: https://www.zyte.com/
   - The `.env` file is already in `.gitignore` to protect your credentials
   - Never commit the `.env` file to version control

3. **Build and run with Docker Compose**
   ```bash
   docker-compose up --build
   ```
   
   The Docker Compose setup will automatically:
   - Load environment variables from `.env` file
   - Set up PostgreSQL database
   - Configure the scraper with your API keys

## Usage

The application is ready to receive:
- Sample Zyte API code for making requests
- Page component structures for Yelp and Yellow Pages

Once these are provided, the scrapers will be fully implemented.

## Database Schema

The `businesses` table stores:
- Basic information (name, source, URL)
- Contact information (phone, email, website)
- Location data (address, city, state, coordinates)
- Business details (category, description, rating, reviews)
- Additional data (hours, amenities, images)
- Metadata (scraped_at, updated_at)

## Development

To run locally without Docker:

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up PostgreSQL database

3. Update `.env` with your database URL

4. Run the application:
   ```bash
   python main.py
   ```

## License

MIT
