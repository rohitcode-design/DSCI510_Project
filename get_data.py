# src/get_data.py
import os
import time
import re
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Selenium (For TikTok)
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Google API (For YouTube)
from googleapiclient.discovery import build

# -------------------------------------------------
#  CONFIGURATION
# -------------------------------------------------
load_dotenv() # Load variables from .env file

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "../data/raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# The 10 Artists
ARTISTS_TO_ANALYZE = [
    {"name": "Taylor Swift", "tiktok_tag": "taylorswift", "youtube_channel_id": "UCqfmriSjJ_k4C8W6J_k7J_g"},
    {"name": "NBA YoungBoy", "tiktok_tag": "nbayoungboy", "youtube_channel_id": "UCNofc_JcK-0FfdJ_YkE6VBg"},
    {"name": "Adele", "tiktok_tag": "adele", "youtube_channel_id": "UCRw-9o3C02JkL4o1CjDkywA"},
    {"name": "Bad Bunny", "tiktok_tag": "badbunny", "youtube_channel_id": "UCgCHiixL-q7L5_Fv2EaV3-w"},
    {"name": "Billie Eilish", "tiktok_tag": "billieeilish", "youtube_channel_id": "UCiGm_E4ZwYVaeYBjfK6edYA"},
    {"name": "Drake", "tiktok_tag": "drake", "youtube_channel_id": "UCByOQJjavOCUDwxCk-jVNRQ"},
    {"name": "The Weeknd", "tiktok_tag": "theweeknd", "youtube_channel_id": "UCOWP5P-ufpRfjbNrmOWwLBQ"},
    {"name": "Doja Cat", "tiktok_tag": "dojacat", "youtube_channel_id": "UCzvK5p4gGg9Q2HfKF3Ab4Qw"},
    {"name": "Post Malone", "tiktok_tag": "postmalone", "youtube_channel_id": "UC3gK4uQkzkG4gQ2vRGLCH3A"},
    {"name": "Kendrick Lamar", "tiktok_tag": "kendricklamar", "youtube_channel_id": "UC31BXkFNkSgWunf6Z64MfKQ"}
]

# -------------------------------------------------
#  TIKTOK SCRAPER (SELENIUM MANUAL ASSIST)
# -------------------------------------------------
def scrape_tiktok_selenium(hashtag):
    """
    Opens a browser for the user to manually verify the page.
    Extracts the 'Post Count' (e.g., '21.5M posts').
    """
    url = f"https://www.tiktok.com/tag/{hashtag}" 
    print(f"\n[TikTok] Launching browser for #{hashtag}...")

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    
    driver = None
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get(url)
        
        # --- MANUAL USER CONFIRMATION ---
        print(f"  >>> ACTION NEEDED: Check the Chrome window.")
        print(f"  1. Close any 'Shop' or 'Login' popups.")
        print(f"  2. Verify you see the number of posts (e.g. '21.5M posts').")
        input(f"  >>> PRESS ENTER HERE when the page is ready for #{hashtag} <<<")
        
        # Scrape
        print("  [Scraping] Reading page text...")
        body_text = driver.find_element(By.TAG_NAME, "body").text
        
        # Regex to find "21.5M posts"
        match = re.search(r'(\d[\d\.]*[KMB]?)\s+posts', body_text, re.IGNORECASE)
        
        if match:
            raw_string = match.group(1).upper()
            print(f"  [Success] Found: {raw_string}")
            
            multiplier = 1
            if 'B' in raw_string: multiplier = 1_000_000_000
            elif 'M' in raw_string: multiplier = 1_000_000
            elif 'K' in raw_string: multiplier = 1_000
            
            clean_str = re.sub(r'[^\d\.]', '', raw_string.replace('B','').replace('M','').replace('K',''))
            post_count = int(float(clean_str) * multiplier)
            return post_count
        else:
            print("  [Error] Could not find 'X posts' text.")
            return 0

    except Exception as e:
        print(f"  [Error] {e}")
        return 0
    finally:
        if driver:
            driver.quit()

# -------------------------------------------------
#  YOUTUBE SCRAPER
# -------------------------------------------------
def get_youtube_data(api_key, channel_id):
    if not api_key:
        print("  [YouTube] No API Key provided (Skipping)")
        return {'view_count': 0, 'subscriber_count': 0}
        
    try:
        yt = build('youtube', 'v3', developerKey=api_key)
        chan_resp = yt.channels().list(part='statistics', id=channel_id).execute()
        if not chan_resp['items']: return None
        
        stats = chan_resp['items'][0]['statistics']
        return {
            'view_count': int(stats.get('viewCount', 0)),
            'subscriber_count': int(stats.get('subscriberCount', 0))
        }
    except Exception as e:
        print(f"  [YouTube] Error: {e}")
        return {'view_count': 0, 'subscriber_count': 0}

# -------------------------------------------------
#  MAIN EXECUTION
# -------------------------------------------------
def run_collection():
    if not YOUTUBE_API_KEY:
        print("WARNING: YOUTUBE_API_KEY is not set in .env. YouTube data will be empty.")

    all_data = []
    print("Starting data collection...")

    for artist in ARTISTS_TO_ANALYZE:
        name = artist['name']
        print(f"\n========== Processing: {name} ==========")
        
        # 1. TikTok (Manual Assist)
        tiktok_posts = scrape_tiktok_selenium(artist['tiktok_tag'])
        
        # 2. YouTube
        yt_stats = get_youtube_data(YOUTUBE_API_KEY, artist['youtube_channel_id'])
        
        # Combine
        summary = {
            'artist': name,
            'tiktok_post_count': tiktok_posts,
            'youtube_total_views': yt_stats.get('view_count', 0),
            'youtube_subs': yt_stats.get('subscriber_count', 0),
            'timestamp': datetime.now().isoformat()
        }
        all_data.append(summary)

    # Save to CSV (Compatible with clean_data.py)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = pd.DataFrame(all_data)
    outfile = os.path.join(RAW_DATA_DIR, f'artists_summary_{timestamp_str}.csv')
    df.to_csv(outfile, index=False)

    print(f"\nSuccess! Data saved to: {outfile}")
    print(df)

if __name__ == "__main__":
    run_collection()
