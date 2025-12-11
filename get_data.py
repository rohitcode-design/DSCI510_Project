# src/get_data.py
import os
import json
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from googleapiclient.discovery import build

# -------------------------------------------------
#  LOAD ENV VARS
# -------------------------------------------------
load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise ValueError("Missing YOUTUBE_API_KEY in .env")

# -------------------------------------------------
#  ARTISTS TO ANALYZE
# -------------------------------------------------
ARTISTS = [
    "Taylor Swift",
    "Drake",
    "Olivia Rodrigo",
    "Doja Cat",
    "Bad Bunny",
    "The Weeknd",
    "SZA",
    "Kendrick Lamar",
    "Billie Eilish",
    "Post Malone"
]

# -------------------------------------------------
#  DIRECTORIES
# -------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "../data/raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# -------------------------------------------------
#  YOUTUBE FUNCTIONS
# -------------------------------------------------
def init_youtube():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def search_youtube_channel(youtube, artist_name):
    """Find official channel ID."""
    req = youtube.search().list(
        q=artist_name,
        type="channel",
        part="snippet",
        maxResults=1
    )
    resp = req.execute()

    if "items" not in resp or len(resp["items"]) == 0:
        return None
    
    return resp["items"][0]["id"]["channelId"]

def fetch_channel_stats(youtube, channel_id):
    req = youtube.channels().list(
        part="statistics,snippet",
        id=channel_id
    )
    resp = req.execute()

    if "items" not in resp or len(resp["items"]) == 0:
        return None

    item = resp["items"][0]
    stats = item["statistics"]
    return {
        "artist_name": item["snippet"]["title"],
        "artist_id": channel_id,
        "subscriber_count": int(stats.get("subscriberCount", 0)),
        "view_count": int(stats.get("viewCount", 0)),
        "video_count": int(stats.get("videoCount", 0)),
        "data_type": "youtube_channel_stats"
    }

def fetch_recent_videos(youtube, channel_id, max_results=20):
    """Fetch basic video metrics"""
    req = youtube.search().list(
        channelId=channel_id,
        part="id,snippet",
        order="date",
        maxResults=max_results
    )
    resp = req.execute()

    videos = []
    for item in resp.get("items", []):
        if item["id"]["kind"] != "youtube#video":
            continue
        
        video_id = item["id"]["videoId"]
        
        # Video stats
        stats_req = youtube.videos().list(
            id=video_id,
            part="statistics,snippet"
        )
        stats_resp = stats_req.execute()

        if "items" not in stats_resp or len(stats_resp["items"]) == 0:
            continue

        vid = stats_resp["items"][0]
        s = vid["statistics"]
        sn = vid["snippet"]

        videos.append({
            "artist_id": channel_id,
            "video_id": video_id,
            "title": sn["title"],
            "published_at": sn["publishedAt"],
            "view_count": int(s.get("viewCount", 0)),
            "like_count": int(s.get("likeCount", 0)),
            "comment_count": int(s.get("commentCount", 0)),
            "data_type": "youtube_video"
        })

    return videos

# -------------------------------------------------
#  TIKTOK SCRAPING FUNCTIONS (HASHTAG METHOD)
# -------------------------------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
}

def scrape_tiktok_hashtag(artist_name):
    """
    Scrapes TikTok via the hashtag page and extracts:
    - total view count
    - total video count
    """

    tag = artist_name.lower().replace(" ", "")
    url = f"https://www.tiktok.com/tag/{tag}?lang=en"

    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            print(f"[TikTok Fail] {artist_name}: HTTP {r.status_code}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Look for <strong data-e2e="challenge-views-count">
        view_el = soup.find("strong", {"data-e2e": "challenge-views-count"})
        if not view_el:
            print(f"[TikTok Missing] No view count found for {artist_name}")
            return None

        views = view_el.text.strip()  # Example: "102.3M"

        def parse_views(v):
            v = v.upper()
            if "B" in v:
                return float(v.replace("B", "")) * 1_000_000_000
            if "M" in v:
                return float(v.replace("M", "")) * 1_000_000
            if "K" in v:
                return float(v.replace("K", "")) * 1_000
            return float(v)

        return {
            "artist_name": artist_name,
            "artist_id": tag,
            "tiktok_hashtag": f"#{tag}",
            "tiktok_views": int(parse_views(views)),
            "data_type": "tiktok_stats"
        }

    except Exception as e:
        print(f"[TikTok Error] {artist_name}: {e}")
        return None

# -------------------------------------------------
#  MASTER FUNCTION TO RUN EVERYTHING
# -------------------------------------------------

def run_collection():
    youtube = init_youtube()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    all_artist_stats = []
    all_content_items = []

    for artist in ARTISTS:
        print(f"\n========== {artist} ==========")

        # ---- YouTube Channel ----
        channel_id = search_youtube_channel(youtube, artist)
        if not channel_id:
            print(f"No YouTube channel found for {artist}")
            continue

        yt_stats = fetch_channel_stats(youtube, channel_id)
        if yt_stats:
            yt_stats["collection_timestamp"] = timestamp
            all_artist_stats.append(yt_stats)

        videos = fetch_recent_videos(youtube, channel_id, max_results=20)
        for v in videos:
            v["collection_timestamp"] = timestamp
            all_content_items.append(v)

        # ---- TikTok ----
        tk = scrape_tiktok_hashtag(artist)
        if tk:
            tk["collection_timestamp"] = timestamp
            all_artist_stats.append(tk)

        time.sleep(1)  # avoid rate limits

    # -------------------------------------------------
    # SAVE SNAPSHOTS
    # -------------------------------------------------
    artists_fp = os.path.join(RAW_DATA_DIR, f"artists_summary_snapshot_{timestamp}.json")
    content_fp = os.path.join(RAW_DATA_DIR, f"content_detail_snapshot_{timestamp}.json")

    with open(artists_fp, "w", encoding="utf-8") as f:
        json.dump(all_artist_stats, f, indent=2)

    with open(content_fp, "w", encoding="utf-8") as f:
        json.dump(all_content_items, f, indent=2)

    print("\n----------------------------------")
    print(" Saved:")
    print("  →", artists_fp)
    print("  →", content_fp)
    print("----------------------------------\n")

# -------------------------------------------------

if __name__ == "__main__":
    run_collection()
