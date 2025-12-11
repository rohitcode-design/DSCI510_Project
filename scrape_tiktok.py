import os
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ---------------------------------------------
#   TikTok Hashtag Scraper (with error handling)
# ---------------------------------------------
def scrape_tiktok_hashtag_stats(hashtag, retries=3, delay=2):
    """
    Scrapes TikTok hashtag page for:
    - Total views (viewCount)
    - Total videos using that tag (videoCount)
    Returns dict or None if fail.
    """

    url = f"https://www.tiktok.com/tag/{hashtag}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            # TikTok sometimes returns blank HTML — retry
            if "props" not in response.text:
                print(f"[Retry {attempt+1}] TikTok page didn’t load properly for #{hashtag}")
                time.sleep(delay)
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            scripts = soup.find_all("script")

            for script in scripts:
                if "viewCount" in script.text and "videoCount" in script.text:
                    text = script.text

                    # Extract viewCount
                    try:
                        view_start = text.index('"viewCount":') + len('"viewCount":')
                        view_end = text.index(",", view_start)
                        view_count = int(text[view_start:view_end].strip())
                    except:
                        view_count = None

                    # Extract videoCount
                    try:
                        vid_start = text.index('"videoCount":') + len('"videoCount":')
                        vid_end = text.index(",", vid_start)
                        video_count = int(text[vid_start:vid_end].strip())
                    except:
                        video_count = None

                    return {
                        "hashtag": hashtag,
                        "tiktok_views": view_count,
                        "tiktok_video_count": video_count
                    }

        except Exception as e:
            print(f"[Error] scraping #{hashtag}: {e}")

        time.sleep(delay + random.uniform(0.5, 2))

    return None


# ------------------------------------------------------
#         Artist List (10 artists, same format)
# ------------------------------------------------------
ARTISTS_TO_ANALYZE = [
    {"name": "Taylor Swift", "tiktok_tag": "taylorswift"},
    {"name": "NBA YoungBoy", "tiktok_tag": "nbayoungboy"},
    {"name": "Adele", "tiktok_tag": "adele"},
    {"name": "Bad Bunny", "tiktok_tag": "badbunny"},
    {"name": "Billie Eilish", "tiktok_tag": "billieeilish"},
    {"name": "Drake", "tiktok_tag": "drake"},
    {"name": "The Weeknd", "tiktok_tag": "theweeknd"},
    {"name": "Doja Cat", "tiktok_tag": "dojacat"},
    {"name": "Post Malone", "tiktok_tag": "postmalone"},
    {"name": "Kendrick Lamar", "tiktok_tag": "kendricklamar"}
]

# ------------------------------------------------------
#           Scrape ALL artists & save to JSON
# ------------------------------------------------------
def scrape_all_artists(artists):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = []

    for artist in artists:
        name = artist["name"]
        tag = artist["tiktok_tag"]

        print(f"\n🔥 Scraping TikTok for {name} (#{tag})...")

        stats = scrape_tiktok_hashtag_stats(tag)

        if stats is None:
            print(f"❌ Failed to scrape {name}")
            stats = {
                "hashtag": tag,
                "tiktok_views": None,
                "tiktok_video_count": None
            }

        stats["artist_name"] = name
        stats["timestamp"] = timestamp

        results.append(stats)

        # Random short delay between artists so TikTok doesn’t suspect bot behavior
        time.sleep(random.uniform(1, 3))

    # Save output
    os.makedirs("data/tiktok", exist_ok=True)
    out_path = f"data/tiktok/tiktok_stats_{timestamp}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4)

    print(f"\n✅ Saved TikTok results to: {out_path}\n")
    return results


# ------------------------------------------------------
#                  Run directly
# ------------------------------------------------------
if __name__ == "__main__":
    scrape_all_artists(ARTISTS_TO_ANALYZE)
