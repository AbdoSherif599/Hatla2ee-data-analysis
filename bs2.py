import csv
import random
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
from pymongo import MongoClient

try:
    import cloudscraper
except ImportError:
    cloudscraper = None


def create_session():
    # Use cloudscraper if available (helps with anti-bot protections), otherwise fallback to requests.
    if cloudscraper:
        session = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
    else:
        session = requests.Session()

    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Connection": "keep-alive",
            "Referer": "https://eg.hatla2ee.com/",
            "Upgrade-Insecure-Requests": "1",
        }
    )
    return session


def fetch_page(session, url, retries=4):
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=30)
        except requests.RequestException as exc:
            wait = 2 ** (attempt - 1)
            print(f"Request error for {url}: {exc}. Retrying in {wait}s...")
            time.sleep(wait)
            continue

        if response.status_code == 200:
            return response

        if response.status_code in {403, 429, 500, 502, 503, 504}:
            wait = 2 ** (attempt - 1)
            print(
                f"Temporary block/failure for {url}, status code: {response.status_code}. "
                f"Retrying in {wait}s (attempt {attempt}/{retries})..."
            )
            time.sleep(wait)
            continue

        print(f"Failed to fetch {url}, status code: {response.status_code}")
        return None

    print(f"Failed after retries: {url}")
    return None


def fetch_description_and_date(session, details_url):
    response = fetch_page(session, details_url, retries=3)
    if response is None:
        return "", ""

    details_soup = BeautifulSoup(response.text, "html.parser")

    desc_tag = details_soup.find("div", id="description")
    description = desc_tag.get_text(" ", strip=True) if desc_tag else ""

    date = ""
    for candidate in details_soup.select("div.font-medium.border-gray-100.w-full.flex"):
        text = candidate.get_text(" ", strip=True)
        match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", text)
        if match:
            date = match.group(0)
            break

    return description, date

# List of brands to scrape
brands = ["toyota", "hyundai", "kia", "mercedes", "renault", "bmw", "chevrolet", "nissan", "opel", "peugeot", "chery", "moris-garage", "fiat", "skoda", "volks-wagen"]

session = create_session()

# Warm up cookies/session once before scraping pages.
try:
    session.get("https://eg.hatla2ee.com/en", timeout=30)
except requests.RequestException:
    pass

# CSV setup - Added 'description' and 'date' to headers
with open("cars.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["brand", "name", "year", "mileage", "transmission", "fuel", "price", "description", "date"])

    for brand in brands:
        print(f"Scraping brand: {brand}")
        for page_num in range(1, 52):  # pages 1 to 51
            url = f"https://eg.hatla2ee.com/en/car/{brand}/page/{page_num}"
            print(f"Fetching URL: {url}")

            response = fetch_page(session, url)
            if response is None:
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            cards = soup.find_all("div", {"data-slot": "card-content"})
            if not cards:
                print(f"No car cards found on {url}")
                break

            for card in cards:
                # Name
                name_tag = card.find("a", class_="no-underline")
                name = name_tag.get_text(strip=True) if name_tag else ""
                details_url = urljoin("https://eg.hatla2ee.com", name_tag.get("href", "")) if name_tag else ""

                # Details: year, mileage, transmission, fuel
                info_divs = card.select("div.text-xs > div.flex.items-center > span")
                year = info_divs[0].get_text(strip=True) if len(info_divs) > 0 else ""
                mileage = info_divs[1].get_text(strip=True) if len(info_divs) > 1 else ""
                transmission = info_divs[2].get_text(strip=True) if len(info_divs) > 2 else ""
                fuel = info_divs[3].get_text(strip=True) if len(info_divs) > 3 else ""

                # Price
                price_tag = card.find("div", class_="text-lg")
                price = price_tag.get_text(strip=True).replace("EGP", "").strip() if price_tag else ""

                description = ""
                date = ""
                if details_url:
                    description, date = fetch_description_and_date(session, details_url)

                # Write row to CSV
                writer.writerow([brand, name, year, mileage, transmission, fuel, price, description, date])

                # Light delay between detail page requests.
                time.sleep(random.uniform(0.4, 0.9))

            # Small jitter to look less bot-like and avoid hammering the site.
            time.sleep(random.uniform(1.0, 2.0))

print("Done! CSV saved as 'cars.csv'.")


