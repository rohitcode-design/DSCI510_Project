import os
import sys
import time
import re
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path so we can import utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.config import ARTISTS_TO_ANALYZE, RAW_DATA_DIR

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Google API
from googleapiclient.discovery import build

# Load Environment Variables
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# --- TIKTOK SCRAPER ---
def scrape_tiktok_selenium(hashtag):
    url = f"https://www.tiktok.com/tag/{hashtag}" 
    print(f"\n[TikTok] Launching browser for #{hashtag}...")

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    
    driver = None
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get(url)
        
        print(f"  >>> ACTION NEEDED: Check the browser window.")
        print(f"  1. Close 'Shop'/'Login' popups.")
        print(f"  2. Verify you see the post count (e.g. '21.5M posts').")
        input(f"  >>> PRESS ENTER HERE when ready for #{hashtag} <<<")
        
        body_text = driver.find_element(By.TAG_NAME, "body").text
        match = re.search(r'(\d[\d\.]*[KMB]?)\s+posts', body_text, re.IGNORECASE)
        
        if match:
            raw = match.group(1).upper()
            print(f"  [Success] Found: {raw}")
            mult = 1
            if 'B' in raw: mult = 1_000_000_000
            elif 'M' in raw: mult = 1_000_000
            elif 'K' in raw: mult = 1_000
            clean = re.sub(r'[^\d\.]', '', raw.replace('B','').replace('M','').replace('K',''))
            return int(float(clean) * mult)
        else:
            print("  [Error] Could not find 'posts' text.")
            return 0
    except Exception as e:
        print(f"  [Error] {e}")
        return 0
    finally:
        if driver: driver.quit()

# --- YOUTUBE SCRAPER (Self-Healing) ---
def get_youtube_data(channel_id, artist_name):
    if not YOUTUBE_API_KEY:
        print("  [YouTube] No API Key found in .env")
        return {'view_count': 0, 'subscriber_count': 0}

    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    
    # Try ID First
    try:
        req = youtube.channels().list(part='statistics', id=channel_id)
        resp = req.execute()
        if 'items' in resp and resp['items']:
            stats = resp['items'][0]['statistics']
            print(f"  [YouTube] Success! {int(stats['viewCount']):,} views.")
            return {
                'view_count': int(stats.get('viewCount', 0)),
                'subscriber_count': int(stats.get('subscriberCount', 0))
            }
    except Exception as e:
        print(f"  [YouTube] ID Warning: {e}")

    # Fallback Search
    print(f"  [YouTube] ID failed. Searching for '{artist_name}'...")
    try:
        search = youtube.search().list(q=artist_name, type='channel', part='id', maxResults=1)
        resp = search.execute()
        if resp['items']:
            new_id = resp['items'][0]['id']['channelId']
            print(f"  [YouTube] Found new ID: {new_id}. Retrying...")
            return get_youtube_data(new_id, "STOP_RECURSION") # Prevent infinite loop
    except Exception as e:
        print(f"  [YouTube] Search failed: {e}")

    return {'view_count': 0, 'subscriber_count': 0}

if __name__ == "__main__":
    data = []
    print("Starting Collection...")
    
    for artist in ARTISTS_TO_ANALYZE:
        print(f"\n=== Processing: {artist['name']} ===")
        tk_posts = scrape_tiktok_selenium(artist['tiktok_tag'])
        yt_stats = get_youtube_data(artist['youtube_channel_id'], artist['name'])
        
        data.append({
            'artist': artist['name'],
            'tiktok_post_count': tk_posts,
            'youtube_total_views': yt_stats['view_count'],
            'youtube_subs': yt_stats['subscriber_count'],
            'timestamp': datetime.now().isoformat()
        })
    
    df = pd.DataFrame(data)
    outfile = os.path.join(RAW_DATA_DIR, f'artists_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    df.to_csv(outfile, index=False)
    print(f"\nSaved raw data to: {outfile}")
