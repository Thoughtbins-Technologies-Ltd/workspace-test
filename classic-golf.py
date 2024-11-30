import time
import logging
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from pymongo import MongoClient
from dotenv import load_dotenv
from deep_translator import GoogleTranslator
import os
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
    collection = db["hotels_classic_golf"]  # Collection name

    # Create a compound index on "hotel_name" and "version" for efficient duplicate tracking
    collection.create_index([("hotel_name", 1), ("version", 1)], unique=True)
    logging.info("Successfully connected to MongoDB and ensured compound index on hotel_name and version.")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

# Configure WebDriver Service and Headless Chrome Options
driver_path = "./chromedriver-win64/chromedriver.exe"  # Path to ChromeDriver
service = Service(driver_path)

chrome_options = Options()
chrome_options.add_argument("--headless")  # Enable headless mode
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")  # Set screen size for better rendering

# Function to translate text
def translate_text(text):
    try:
        return GoogleTranslator(source='de', target='en').translate(text)
    except Exception as e:
        logging.warning(f"Translation failed for text: {text}. Error: {e}")
        return text  # Return original text if translation fails

# Function to search hotel and extract translated table data
def search_hotel_and_extract_data(hotel_name, driver):
    try:
        # Open the search page
        search_url = "https://www.classicgolftours.de/search"
        driver.get(search_url)

        # Handle the cookie consent dialog
        try:
            cookie_accept_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"))
            )
            cookie_accept_button.click()
            logging.info("Cookie consent dialog closed.")
        except Exception as e:
            logging.warning("No cookie consent dialog found or error closing it. Proceeding...")

        # Locate the input field and fill in the hotel name
        email_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#email"))
        )
        email_input.clear()
        email_input.send_keys(hotel_name)

        # Submit the form
        submit_button = driver.find_element(By.CSS_SELECTOR, "#mainContent > div.centeredContainer > div > div > form > div > button")
        submit_button.click()

        # Wait for results to load
        time.sleep(5)

        # Locate the "Hotels" section and get the first result link
        soup = BeautifulSoup(driver.page_source, "html.parser")
        hotel_links = soup.select("#region > div:nth-child(1) > div > div > ul > li > a")

        if hotel_links:
            relative_url = hotel_links[0]["href"]
            final_url = f"https://www.classicgolftours.de{relative_url}#preise"
            logging.info(f"Final Hotel URL with #preise for '{hotel_name}': {final_url}")

            # Open the final URL to scrape pricing data
            driver.get(final_url)
            time.sleep(5)  # Wait for the page to load

            # Parse the page content
            soup = BeautifulSoup(driver.page_source, "html.parser")
            price_table = soup.select_one("#text_preise > div:nth-child(2) > table > tbody")

            if price_table:
                # Extract and translate table data as text
                table_data = []
                for row in price_table.find_all("tr"):
                    cells = [translate_text(cell.get_text(strip=True)) for cell in row.find_all(["th", "td"])]
                    table_data.append(cells)

                logging.info(f"Extracted and translated table data for '{hotel_name}'.")
                return {
                    "hotel_name": hotel_name,
                    "hotel_url": final_url,
                    "table_data": table_data,
                    "timestamp": datetime.now(timezone.utc)
                }
            else:
                logging.warning(f"No price table found for '{hotel_name}' at URL: {final_url}")
        else:
            logging.warning(f"No hotel links found for '{hotel_name}'.")
        return None
    except Exception as e:
        logging.error(f"An error occurred while processing '{hotel_name}': {e}")
        return None

# Function to process hotels from JSON file
def process_hotels_from_json(json_file):
    try:
        # Load hotel names from JSON file
        with open(json_file, "r", encoding="utf-8") as file:
            hotels = json.load(file)

        # Initialize WebDriver with headless mode
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # Process each hotel
        for hotel_name in hotels:
            logging.info(f"Processing hotel: {hotel_name}")
            extracted_data = search_hotel_and_extract_data(hotel_name, driver)
            if extracted_data:
                # Store in MongoDB
                version = collection.count_documents({"hotel_name": hotel_name}) + 1
                extracted_data["version"] = version  # Add version to the document
                try:
                    collection.insert_one(extracted_data)
                    logging.info(f"Data successfully stored in MongoDB for hotel: {hotel_name}, version: {version}")
                except Exception as e:
                    logging.warning(f"Failed to store data in MongoDB for hotel: {hotel_name}. Error: {e}")

        # Close the browser after processing all hotels
        driver.quit()

    except Exception as e:
        logging.error(f"An error occurred while processing the hotels JSON: {e}")
        if "driver" in locals():
            driver.quit()

# Main execution
if __name__ == "__main__":
    json_file = "hotels.json"  # JSON file containing hotel names
    process_hotels_from_json(json_file)
