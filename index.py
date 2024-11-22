import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# MongoDB setup
mongo_uri = os.getenv("MONGO_URI")  # Replace with your MongoDB URI if using MongoDB Atlas
client = MongoClient(mongo_uri)
db = client["hotel_data"]  # Database name
collection = db["hotels"]  # Collection name

# Base URL and hotel name
base_url = "https://www.golf-extra.com"
hotel_name = "The Westin Resort Costa Navarino"  # Change this to your target hotel

# Step 1: Search for the hotel and get the hotel page URL
def search_hotel(base_url, hotel_name):
    # Construct the search URL with the hotel name
    search_url = f"{base_url}/suche?tx_solr%5Bq%5D={hotel_name.replace(' ', '+')}"
    
    try:
        # Send GET request to the search URL
        response = requests.get(search_url)
        response.raise_for_status()  # Raise an error for bad HTTP responses
        
        # Parse the search result page
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the specific hotel link (adjust the selectors based on actual HTML)
        hotel_link = None
        for link in soup.find_all('a', href=True):
            if hotel_name.lower() in link.text.lower():
                hotel_link = link['href']
                break
        
        if hotel_link:
            # Full hotel URL
            full_hotel_url = f"{base_url}{hotel_link}"
            print(f"Found hotel page: {full_hotel_url}")
            return full_hotel_url
        else:
            print(f"No link found for hotel: {hotel_name}")
    except requests.exceptions.RequestException as e:
        print(f"Error during HTTP request: {e}")
    return None

# Step 2: Scrape data from the hotel page and save to MongoDB
def scrape_and_save_to_mongodb(hotel_url, selectors):
    try:
        response = requests.get(hotel_url)
        response.raise_for_status()  # Raise an error for bad HTTP responses

        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Create a dictionary to store data extracted by each selector
        extracted_data = {}

        # Loop through the list of selectors and extract data
        for selector in selectors:
            target_data = soup.select(selector)
            data_list = [element.text.strip() for element in target_data]
            extracted_data[selector] = data_list

        if extracted_data:
            # Insert data into MongoDB
            collection.insert_one({
                "hotel_url": hotel_url,
                "data": extracted_data
            })
            print(f"Data successfully saved to MongoDB: {extracted_data}")
        else:
            print(f"No data extracted from the page: {hotel_url}")

    except requests.exceptions.RequestException as e:
        print(f"Error during HTTP request: {e}")

# Execute the script
hotel_page_url = search_hotel(base_url, hotel_name)
if hotel_page_url:
    # CSS selectors for extracting data
    selectors = [
        "#ge-hotel-information > div > div > div.col-lg-6.d-flex.mb-5.mb-lg-0 > div",
        "#ge-hotel-information > div > div > div:nth-child(2) > div",
    ]  # Adjust selectors based on the actual structure of the hotel page
    
    # Scrape the hotel page and save data to MongoDB
    scrape_and_save_to_mongodb(hotel_page_url, selectors)
else:
    print("Hotel page not found. Exiting script.")
