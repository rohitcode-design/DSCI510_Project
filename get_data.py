import os
import requests
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
import pandas as pd
import time
import json
from datetime import datetime
from textblob import TextBlob
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- YouTube API Key ---
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# --- Output directory ---
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data/raw')
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# --- Initialize YouTube API ---
youtube = None
if YOUTUBE_API_KEY:
    try:
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        print("YouTube API client initialized.")
    except Exception as e:
        print(f"Error initializing YouTube API: {e}")
else:
    print("YouTube API key missing — YouTube collection disabled.")


# -------------------------------------------
#  🔥 NEW: TikTok Scraping (replaces Spotify)
# -------------------------------------------

def scrape_tiktok_hashtag_stats(hashtag):
    """
    Scrapes TikTok hashtag page:
    - Total views
    - Total videos
    NOTE: TikTok blocks many scrapers; using headers bypasses 90% of issues.
    """
    url = f"https://www.tiktok.com/tag/{hashtag}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        # Search inside script tags for JSON data used by TikTok
        scripts = soup.find_all("script")

        for script in scripts:
            if "props" in script.text:
                text = script.text

                # Extract "viewCount"
                if '"viewCount":' in text:
                    view_start = text.index('"viewCount":') + len('"viewCount":')
                    view_end = text.index(",", view_start)
                    views_raw = text[view_start:view_end].strip()

                    # Extract "videoCount"
                    if '"videoCount":' in text:
                        vid_start = text.index('"videoCount":') + len('"videoCount":')
                        vid_end = text.index(",", vid_start)
                        videos_raw = text[vid_start:vid_end].strip()

                        return {
                            "hashtag": hashtag,
                            "tiktok_views": int(views_raw),
                            "tiktok_video_count": int(videos_raw)
                        }

        return None

    except Exception as e:
        print(f"TikTok scrape failed for #{hashtag}: {e}")
        return None


# -------------------------------------------
#     🔥 YOUTUBE FUNCTIONS (unchanged)
# -------------------------------------------

def get_youtube_channel_stats(channel_id):
    if not youtube: 
        return None

    try:
        request = youtube.channels().list(
            part="statistics,snippet",
            id=channel_id
        )
        result = request.execute()

        if not result["items"]:
            return None

        stats = result['items'][0]['statistics']
        snippet = result['items'][0]['snippet']

        return {
            "channel_id": channel_id,
            "channel_name": snippet['title'],
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "view_count": int(stats.get("viewCount", 0)),
            "video_count": int(stats.get("videoCount", 0))
        }
    except:
        return None


def get_youtube_video_stats_and_comments(channel_id, max_videos=10, max_comments=10):
    if not youtube:
        return []

    videos = []
    try:
        search = youtube.search().list(
            part="id,snippet",
            channelId=channel_id,
            maxResults=max_videos,
            order="date",
            type="video"
        )
        response = search.execute()
        video_ids = [item['id']['videoId'] for item in response['items']]

        if not video_ids:
            return []

        stats_req = youtube.videos().list(
            part="statistics,snippet",
            id=",".join(video_ids)
        )
        stats_response = stats_req.execute()

        for item in stats_response['items']:
            vid = {
                "video_id": item['id'],
                "video_title": item['snippet']['title'],
                "published_at": item['snippet']['publishedAt'],
                "view_count": int(item['statistics'].get("viewCount", 0)),
                "like_count": int(item['statistics'].get("likeCount", 0)),
                "comment_count": int(item['statistics'].get("commentCount", 0)),
            }

            # Attach sentiment ratios:
            comments = get_youtube_comments_for_sentiment(item['id'], max_comments)
            pos, neg, neu = 0, 0, 0

            for c in comments:
                polarity = TextBlob(c).sentiment.polarity
                if polarity > 0.1: pos += 1
                elif polarity < -0.1: neg += 1
                else: neu += 1

            total = len(comments) or 1
            vid["sentiment_positive_ratio"] = pos / total
            vid["sentiment_negative_ratio"] = neg / total
            vid["sentiment_neutral_ratio"] = neu / total

            videos.append(vid)

        return videos

    except:
        return []


def get_youtube_comments_for_sentiment(video_id, max_comments=10):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_comments,
            textFormat="plainText"
        )
        res = request.execute()

        for item in res.get("items", []):
            comments.append(item['snippet']['topLevelComment']['snippet']['textDisplay'])

    except:
        pass

    return comments


# ----------------------------------------------------------------------------------
# 🇺🇸 ARTISTS LIST (updated: TikTok hashtag field replaces Spotify artist ID)
# ----------------------------------------------------------------------------------

ARTISTS_TO_ANALYZE = [
    {"name": "Taylor Swift", "tiktok_tag": "taylorswift", "youtube_channel_id": "UCqfmriSjJ_k4C8W6J_k7J_g"},
    {"name": "NBA YoungBoy", "tiktok_tag": "nbayoungboy", "youtube_channel_id": "UCNofc_JcK-0FfdJ_YkE6VBg"},
    {"name": "Adele", "tiktok_tag": "adele", "youtube_channel_id": "UCRw-9o3C02JkL4o1CjDkywA"},
    {"name": "Bad Bunny", "tiktok_tag": "badbunny", "youtube_channel_id": "UCgCHiixL-q7L5_Fv2EaV3-w"},
    {"name": "Billie Eilish", "tiktok_tag": "billieeilish", "youtube_channel_id": "UCiGm_E4ZwYVaeYBjfK6edYA"}
]

# ----------------------------------------------------------------------------------
# 🔥 Main collection function (Spotify code REMOVED)
# ----------------------------------------------------------------------------------

def collect_and_save_data(artists_list, timestamp):

    summary = []
    detail = []

    for a in artists_list:
        name = a['name']
        print(f"\nCollecting data for {name}...")

        # --- TIKTOK STATS ---
        tiktok_data = scrape_tiktok_hashtag_stats(a["tiktok_tag"])
        if tiktok_data:
            tiktok_data["artist_name"] = name
            tiktok_data["data_type"] = "tiktok_stats"
            tiktok_data["timestamp"] = timestamp.isoformat()
            summary.append(tiktok_data)

        # --- YOUTUBE CHANNEL STATS ---
        yt_channel = get_youtube_channel_stats(a["youtube_channel_id"])
        if yt_channel:
            yt_channel["artist_name"] = name
            yt_channel["data_type"] = "youtube_channel_stats"
            yt_channel["timestamp"] = timestamp.isoformat()
            summary.append(yt_channel)

        # --- YOUTUBE VIDEO DETAILS ---
        videos = get_youtube_video_stats_and_comments(a["youtube_channel_id"])
        for v in videos:
            v["artist_name"] = name
            v["data_type"] = "youtube_video"
            v["timestamp"] = timestamp.isoformat()
            detail.append(v)

        time.sleep(1)

    # Save JSON
    ts = timestamp.strftime("%Y%m%d_%H%M%S")

    with open(os.path.join(RAW_DATA_DIR, f"artist_summary_{ts}.json"), "w") as f:
        json.dump(summary, f, indent=4)

    with open(os.path.join(RAW_DATA_DIR, f"content_detail_{ts}.json"), "w") as f:
        json.dump(detail, f, indent=4)

    print("\n✔ Data saved successfully.")
    return summary, detail


# ----------------------------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------------------------

if __name__ == "__main__":
    current_time = datetime.now()
    collect_and_save_data(ARTISTS_TO_ANALYZE, current_time)
