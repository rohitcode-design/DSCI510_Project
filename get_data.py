import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from googleapiclient.discovery import build
import pandas as pd
import time
import json
from datetime import datetime
from textblob import TextBlob # For sentiment analysis
# For loading environment variables from a .env file (optional but recommended)
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# --- Configuration (API Keys - loaded from environment variables) ---
# IMPORTANT: Ensure your environment variables are set or a .env file is present.
# The `os.getenv` calls will return None if not set, leading to API initialization errors.
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# --- Output Paths ---
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data/raw')
os.makedirs(RAW_DATA_DIR, exist_ok=True) # Create raw data directory if it doesn't exist

# --- Initialize API Clients ---
# Spotify
sp = None # Initialize to None
if SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET:
    try:
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=SPOTIPY_CLIENT_ID,
            client_secret=SPOTIPY_CLIENT_SECRET
        ))
        print("Spotify API client initialized.")
    except Exception as e:
        print(f"Error initializing Spotify API: {e}. Check SPOTIPY_CLIENT_ID/SECRET in .env or environment variables.")
else:
    print("Spotify API credentials not found. Skipping Spotify data collection.")


# YouTube
youtube = None # Initialize to None
if YOUTUBE_API_KEY:
    try:
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        print("YouTube API client initialized.")
    except Exception as e:
        print(f"Error initializing YouTube API: {e}. Check YOUTUBE_API_KEY in .env or environment variables.")
else:
    print("YouTube API key not found. Skipping YouTube data collection.")


# --- List of artists to analyze ---
# Ensure you have valid Spotify and YouTube IDs.
# Find Spotify IDs: Search on Spotify, click 'Share' -> 'Copy Spotify URI', then extract ID.
# Find YouTube Channel IDs: Go to channel page, URL is often youtube.com/channel/CHANNEL_ID
ARTISTS_TO_ANALYZE = [
    {"name": "Taylor Swift", "spotify_id": "06HL4zAwFN824YGZdf22xy", "youtube_channel_id": "UCqfmriSjJ_k4C8W6J_k7J_g"},
    {"name": "NBA YoungBoy", "spotify_id": "7wlFDEWiM5OoIHgQcfapFV", "youtube_channel_id": "UCNofc_JcK-0FfdJ_YkE6VBg"},
    {"name": "Adele", "spotify_id": "4dpARuHxo51G3z76HENDSmi", "youtube_channel_id": "UCRw-9o3C02JkL4o1CjDkywA"},
    {"name": "Metallica", "spotify_id": "2ye2Wgw4gimzAp22BptqmN", "youtube_channel_id": "UCGf4G0Wp2n3HlXg6S-1V6Yg"},
    {"name": "Bad Bunny", "spotify_id": "4q3ewfiRJz2DCgBhPksYn6", "youtube_channel_id": "UCgCHiixL-q7L5_Fv2EaV3-w"},
    {"name": "Billie Eilish", "spotify_id": "6oSJUSadgVQVl5PPMBKaHc", "youtube_channel_id": "UCiGm_E4ZwYVaeYBjfK6edYA"}
]

# --- Helper Functions for API Calls ---

def get_spotify_artist_profile_data(artist_id):
    """Fetches Spotify artist profile data (followers, genres, overall popularity)."""
    if not sp: return None # Check if Spotify client was initialized
    try:
        artist = sp.artist(artist_id)
        return {
            'artist_id': artist_id,
            'artist_name': artist['name'],
            'followers': artist['followers']['total'],
            'genres': artist['genres'],
            'popularity_score': artist['popularity'] # 0-100 score
        }
    except Exception as e:
        print(f"Error fetching Spotify profile data for {artist_id}: {e}")
        return None

def get_spotify_artist_top_tracks(artist_id, limit=20): # Increased limit for more data
    """Fetches a limited number of an artist's top tracks with release dates."""
    if not sp: return [] # Check if Spotify client was initialized
    tracks_data = []
    try:
        results = sp.artist_top_tracks(artist_id)
        for track in results['tracks'][:limit]:
            # Some tracks might not have album info (e.g., singles) or release_date
            release_date = track['album'].get('release_date') if 'album' in track else None
            tracks_data.append({
                'track_id': track['id'],
                'track_name': track['name'],
                'album_name': track['album']['name'] if 'album' in track else 'N/A',
                'release_date': release_date,
                'track_popularity': track['popularity'] # 0-100 score
            })
        return tracks_data
    except Exception as e:
        print(f"Error fetching Spotify top tracks for {artist_id}: {e}")
        return []

def get_youtube_channel_stats(channel_id):
    """Fetches YouTube channel statistics (subscribers, total views, video count)."""
    if not youtube: return None # Check if YouTube client was initialized
    try:
        request = youtube.channels().list(
            part="statistics,snippet",
            id=channel_id
        )
        response = request.execute()
        if response['items']:
            stats = response['items'][0]['statistics']
            snippet = response['items'][0]['snippet']
            return {
                'channel_id': channel_id,
                'channel_name': snippet['title'],
                'subscriber_count': int(stats.get('subscriberCount', 0)),
                'view_count': int(stats.get('viewCount', 0)),
                'video_count': int(stats.get('videoCount', 0))
            }
        return None
    except Exception as e:
        print(f"Error fetching YouTube channel data for {channel_id}: {e}")
        return None

def get_youtube_video_stats_and_comments(channel_id, max_videos=20, max_comments_per_video=10): # Increased limits
    """
    Fetches stats for a limited number of recent videos from a channel
    and performs sentiment analysis on a sample of their comments.
    """
    if not youtube: return [] # Check if YouTube client was initialized
    videos_data = []
    try:
        # 1. Get recent video IDs
        search_request = youtube.search().list(
            part="id,snippet",
            channelId=channel_id,
            maxResults=max_videos,
            order="date", # Order by date to get recent videos
            type="video"
        )
        search_response = search_request.execute()
        video_ids = [item['id']['videoId'] for item in search_response['items'] if 'videoId' in item['id']]

        if not video_ids:
            return []

        # 2. Get statistics and snippet for these videos
        # The API allows up to 50 video IDs per call
        video_stats_request = youtube.videos().list(
            part="statistics,snippet",
            id=",".join(video_ids)
        )
        video_stats_response = video_stats_request.execute()

        for item in video_stats_response['items']:
            video_id = item['id']
            stats = item.get('statistics', {})
            snippet = item['snippet']

            video_info = {
                'video_id': video_id,
                'video_title': snippet['title'],
                'published_at': snippet['publishedAt'], # Crucial for temporal analysis
                'view_count': int(stats.get('viewCount', 0)),
                'like_count': int(stats.get('likeCount', 0)),
                'comment_count': int(stats.get('commentCount', 0)),
            }

            # 3. Get comments for sentiment analysis
            comments = get_youtube_comments_for_sentiment(video_id, max_comments_per_video)
            if comments:
                positive_score = 0
                negative_score = 0
                neutral_score = 0
                for comment_text in comments:
                    analysis = TextBlob(comment_text)
                    # Use thresholds to classify sentiment
                    if analysis.sentiment.polarity > 0.1:
                        positive_score += 1
                    elif analysis.sentiment.polarity < -0.1:
                        negative_score += 1
                    else:
                        neutral_score += 1
                total_analyzed = len(comments)
                video_info['sentiment_positive_ratio'] = positive_score / total_analyzed if total_analyzed > 0 else 0
                video_info['sentiment_negative_ratio'] = negative_score / total_analyzed if total_analyzed > 0 else 0
                video_info['sentiment_neutral_ratio'] = neutral_score / total_analyzed if total_analyzed > 0 else 0
            else:
                video_info['sentiment_positive_ratio'] = 0
                video_info['sentiment_negative_ratio'] = 0
                video_info['sentiment_neutral_ratio'] = 0

            videos_data.append(video_info)
        return videos_data
    except Exception as e:
        print(f"Error fetching YouTube video data for channel {channel_id}: {e}")
        return []

def get_youtube_comments_for_sentiment(video_id, max_comments=10):
    """Fetches a small sample of comments for a given YouTube video."""
    if not youtube: return [] # Check if YouTube client was initialized
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=min(max_comments, 100), # YouTube API max 100 results per call
            textFormat="plainText"
        )
        response = request.execute()
        for item in response['items']:
            comments.append(item['snippet']['topLevelComment']['snippet']['textDisplay'])
            if len(comments) >= max_comments:
                break
    except Exception as e:
        # This can happen if comments are disabled for a video, or other API errors
        # print(f"Could not retrieve comments for video {video_id}: {e}") # Uncomment for debugging
        pass
    return comments

def collect_and_save_data(artists_list, collection_timestamp):
    """
    Orchestrates data collection for all artists and saves raw JSON files.
    This function performs a single snapshot data collection.
    Temporal insights will be derived from metadata (e.g., video/track published_at dates)
    rather than repeated live collection over time.
    """
    all_artists_summary = [] # For overall artist metrics like followers, subscribers
    all_content_detail = [] # For individual YouTube videos and Spotify tracks

    print(f"Starting data collection (snapshot) for {collection_timestamp}...")
    for artist_info in artists_list:
        artist_name = artist_info['name']
        print(f"  Collecting data for: {artist_name}...")

        # --- Spotify Data ---
        if sp: # Only try to collect if Spotify client was initialized
            # 1. Artist Profile Data (Followers, overall popularity, genres)
            spotify_profile_data = get_spotify_artist_profile_data(artist_info['spotify_id'])
            if spotify_profile_data:
                spotify_profile_data['data_type'] = 'artist_profile'
                spotify_profile_data['collection_timestamp'] = collection_timestamp.isoformat()
                all_artists_summary.append(spotify_profile_data)

            # 2. Artist Top Tracks Data (for release dates and track-level popularity)
            spotify_tracks_data = get_spotify_artist_top_tracks(artist_info['spotify_id'])
            for track in spotify_tracks_data:
                track['artist_name'] = artist_name
                track['artist_id'] = artist_info['spotify_id']
                track['data_type'] = 'spotify_track'
                track['collection_timestamp'] = collection_timestamp.isoformat()
                all_content_detail.append(track)


        # --- YouTube Data ---
        if youtube: # Only try to collect if YouTube client was initialized
            # 1. Channel Statistics (Subscribers, total views, video count)
            youtube_channel_data = get_youtube_channel_stats(artist_info['youtube_channel_id'])
            if youtube_channel_data:
                youtube_channel_data['artist_name'] = artist_name
                youtube_channel_data['artist_id'] = artist_info['spotify_id'] # Use Spotify ID as a common artist identifier
                youtube_channel_data['data_type'] = 'youtube_channel_stats'
                youtube_channel_data['collection_timestamp'] = collection_timestamp.isoformat()
                all_artists_summary.append(youtube_channel_data)

            # 2. Recent Videos Data (Views, likes, comments, sentiment, published_at)
            youtube_videos_data = get_youtube_video_stats_and_comments(
                artist_info['youtube_channel_id']
            )
            for video in youtube_videos_data:
                video['artist_name'] = artist_name
                video['artist_id'] = artist_info['spotify_id']
                video['channel_id'] = artist_info['youtube_channel_id']
                video['data_type'] = 'youtube_video'
                video['collection_timestamp'] = collection_timestamp.isoformat()
                all_content_detail.append(video)

        # Pause to respect API rate limits and avoid hitting them too quickly
        time.sleep(1) # Delay between artists

    # Save raw data to JSON files
    timestamp_str = collection_timestamp.strftime("%Y%m%d_%H%M%S")

    # Save summary data (artist profiles, channel stats)
    artists_summary_filepath = os.path.join(RAW_DATA_DIR, f'artists_summary_snapshot_{timestamp_str}.json')
    with open(artists_summary_filepath, 'w', encoding='utf-8') as f:
        json.dump(all_artists_summary, f, ensure_ascii=False, indent=4)
    print(f"Saved artist summary data to: {artists_summary_filepath}")

    # Save detailed content data (Spotify tracks, YouTube videos)
    content_detail_filepath = os.path.join(RAW_DATA_DIR, f'content_detail_snapshot_{timestamp_str}.json')
    with open(content_detail_filepath, 'w', encoding='utf-8') as f:
        json.dump(all_content_detail, f, ensure_ascii=False, indent=4)
    print(f"Saved content detail data to: {content_detail_filepath}")

    print("Data collection complete.")
    return all_artists_summary, all_content_detail

if __name__ == "__main__":
    # --- IMPORTANT ---
    # This script will perform a SINGLE data collection run (a snapshot).
    # Since the project's temporal analysis focuses on content release dates
    # rather than live growth tracking over weeks (due to potential waning popularity),
    # you typically run this once to gather the current state and historical metadata
    # available via APIs.
    # The 'published_at' for YouTube videos and 'release_date' for Spotify tracks
    # will be used in subsequent analysis steps (in run_analysis.py) to derive
    # temporal insights like content longevity and release frequency.

    # Ensure NLTK data is downloaded for TextBlob sentiment analysis
    import nltk
    print("Checking for NLTK data...")
    try:
        # TextBlob primarily uses 'punkt' and 'averaged_perceptron_tagger'
        nltk.data.find('tokenizers/punkt')
        nltk.data.find('taggers/averaged_perceptron_tagger')
    except nltk.downloader.DownloadError:
        print("Downloading NLTK data ('punkt', 'averaged_perceptron_tagger')...")
        nltk.download('punkt')
        nltk.download('averaged_perceptron_tagger')
        print("NLTK data download complete.")

    current_collection_time = datetime.now()
    collect_and_save_data(ARTISTS_TO_ANALYZE, current_collection_time)
