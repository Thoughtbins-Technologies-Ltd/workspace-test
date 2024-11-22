import os
import logging
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from dotenv import load_dotenv
from deep_translator import GoogleTranslator
from datetime import datetime
from datetime import datetime, timezone
# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MongoDB setup
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("MongoDB URI not found in environment variables! Please check your .env file.")

try:
    client = MongoClient(mongo_uri)
    db = client["hotel_data"]  # Database name
    collection = db["hotels_eng"]  # Collection name

    # Create a compound index on "hotel_url" and "version" for efficient duplicate tracking
    collection.create_index([("hotel_url", 1), ("version", 1)], unique=True)
    logging.info("Successfully connected to MongoDB and ensured compound index on hotel_url and version.")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

# Base URL and hotel name
base_url = "https://www.golf-extra.com"
hotel_name = "The Westin Resort Costa Navarino"

# Function to translate text from German to English
def translate_to_english(text):
    try:
        if not text.strip():  # Skip empty strings
            return text
        return GoogleTranslator(source='de', target='en').translate(text.strip())  # Strip white spaces before translating
    except Exception as e:
        logging.warning(f"Translation failed for text: {text} | Error: {e}")
        return text  # Return original text if translation fails

# Step 1: Search for the hotel and get the hotel page URL
def search_hotel(base_url, hotel_name):
    search_url = f"{base_url}/suche?tx_solr%5Bq%5D={hotel_name.replace(' ', '+')}"
    try:
        response = requests.get(search_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the specific hotel link
        hotel_link = None
        for link in soup.find_all('a', href=True):
            if hotel_name.lower() in link.text.lower():
                hotel_link = link['href']
                break

        if hotel_link:
            full_hotel_url = f"{base_url}{hotel_link}"
            logging.info(f"Found hotel page: {full_hotel_url}")
            return full_hotel_url
        else:
            logging.warning(f"No link found for hotel: {hotel_name}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during HTTP request: {e}")
        return None



# Step 2: Scrape data from the hotel page, clean, translate to English, and save to MongoDB
def scrape_and_save_to_mongodb(hotel_url, selectors):
    try:
        response = requests.get(hotel_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract data
        extracted_data = {}
        for idx, selector in enumerate(selectors):
            target_data = soup.select(selector)
            data_list = [translate_to_english(element.text.strip()) for element in target_data if element.text.strip()]

            # Key-value pair assignment
            extracted_data[f"section_{idx + 1}"] = data_list

        if extracted_data:
            # Get the current version for the hotel_url
            version = collection.count_documents({"hotel_url": hotel_url}) + 1
            document = {
                "hotel_url": hotel_url,
                "data": extracted_data,
                "version": version,
                "timestamp": datetime.now(timezone.utc)  # Use timezone-aware datetime
            }
            try:
                # Attempt to insert the document
                collection.insert_one(document)
                logging.info(f"Translated and cleaned data successfully saved to MongoDB for URL: {hotel_url}, version: {version}")
            except pymongo.errors.DuplicateKeyError:
                logging.warning(f"Duplicate key error for hotel_url: {hotel_url}. Retrying with a new version.")
        else:
            logging.warning(f"No data extracted from the page: {hotel_url}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error during HTTP request: {e}")
