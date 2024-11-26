import os
import logging
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, errors  # Ensure pymongo is imported
from dotenv import load_dotenv
from deep_translator import GoogleTranslator
from datetime import datetime, timezone
import json

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
    collection = db["hotels_golf-motion"]  # Collection name

    # Create a compound index on "hotel_url" and "version" for efficient duplicate tracking
    collection.create_index([("hotel_url", 1), ("version", 1)], unique=True)
    logging.info("Successfully connected to MongoDB and ensured compound index on hotel_url and version.")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

# Base URL for the new site
base_url = "https://www.golfmotion.com"

# Function to translate text from German to English
def translate_to_english(text):
    try:
        if not text.strip():  # Skip empty strings
            return text
        # Translate only the first 500 characters to avoid issues with large chunks
        return GoogleTranslator(source='de', target='en').translate(text.strip()[:500])  
    except Exception as e:
        logging.warning(f"Translation failed for text: {text[:100]}... | Error: {e}")
        return text  # Return original text if translation fails

# Construct hotel URL directly
def construct_hotel_url(base_url, hotel_name):
    # Replace spaces with dashes and convert to lowercase to match URL format
    formatted_name = hotel_name.lower().replace(' ', '-')
    hotel_url = f"{base_url}/{formatted_name}.html"
    logging.info(f"Constructed URL: {hotel_url}")
    return hotel_url

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
            except errors.DuplicateKeyError:
                logging.warning(f"Duplicate key error for hotel_url: {hotel_url}. Retrying with a new version.")
        else:
            logging.warning(f"No data extracted from the page: {hotel_url}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error during HTTP request: {e}")

# Step 3: Load JSON with hotel names and process them
def process_bulk_hotels(json_file, selectors):
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            hotels = json.load(f)

        for hotel_name in hotels:
            logging.info(f"Processing hotel: {hotel_name}")
            hotel_page_url = construct_hotel_url(base_url, hotel_name)
            scrape_and_save_to_mongodb(hotel_page_url, selectors)
    except Exception as e:
        logging.error(f"Failed to process hotels from JSON file: {e}")

# Example usage
if __name__ == "__main__":
    selectors = [
        "#hoteldetail > div > div > div:nth-child(3)",
        "#hoteldetail > div > div > div:nth-child(4)"  # Add more as required
    ]  # Adjust selectors based on the actual structure of the hotel page

    # JSON file containing hotel names
    hotel_json_file = "hotels.json"  # Make sure this file is in the correct path

    process_bulk_hotels(hotel_json_file, selectors)
