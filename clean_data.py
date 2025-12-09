# src/clean_data.py
import pandas as pd
import os
import json
from datetime import datetime

# --- Configuration ---
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data/raw')
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data/processed')
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True) # Ensure processed data directory exists

def load_raw_data():
    """
    Loads the most recent raw JSON data files from data/raw.
    Assumes file names are in the format:
    artists_summary_snapshot_YYYYMMDD_HHMMSS.json
    content_detail_snapshot_YYYYMMDD_HHMMSS.json
    """
    raw_artists_data = []
    raw_content_data = []

    # Find the most recent snapshot files
    artist_files = sorted([f for f in os.listdir(RAW_DATA_DIR) if f.startswith('artists_summary_snapshot_') and f.endswith('.json')], reverse=True)
    content_files = sorted([f for f in os.listdir(RAW_DATA_DIR) if f.startswith('content_detail_snapshot_') and f.endswith('.json')], reverse=True)

    if not artist_files or not content_files:
        print("No raw data files found. Please run get_data.py first.")
        return pd.DataFrame(), pd.DataFrame()

    most_recent_artist_file = os.path.join(RAW_DATA_DIR, artist_files[0])
    most_recent_content_file = os.path.join(RAW_DATA_DIR, content_files[0])

    print(f"Loading raw artists data from: {most_recent_artist_file}")
    with open(most_recent_artist_file, 'r', encoding='utf-8') as f:
        raw_artists_data = json.load(f)

    print(f"Loading raw content data from: {most_recent_content_file}")
    with open(most_recent_content_file, 'r', encoding='utf-8') as f:
        raw_content_data = json.load(f)

    df_artists = pd.DataFrame(raw_artists_data)
    df_content = pd.DataFrame(raw_content_data)

    return df_artists, df_content

def clean_and_process_data(df_artists_raw, df_content_raw):
    """
    Cleans and processes raw artist and content data.
    """
    print("Starting data cleaning and processing...")

    # --- Clean Artists Data ---
    # Filter for Spotify artist profiles and YouTube channel stats separately
    df_spotify_artists = df_artists_raw[df_artists_raw['data_type'] == 'artist_profile'].copy()
    df_youtube_channels = df_artists_raw[df_artists_raw['data_type'] == 'youtube_channel_stats'].copy()

    # Rename columns for clarity and merge preparation
    df_spotify_artists = df_spotify_artists[['artist_name', 'artist_id', 'followers', 'popularity_score', 'genres', 'collection_timestamp']]
    df_spotify_artists.rename(columns={'followers': 'spotify_followers', 'popularity_score': 'spotify_popularity'}, inplace=True)

    df_youtube_channels = df_youtube_channels[['artist_name', 'artist_id', 'subscriber_count', 'view_count', 'video_count', 'collection_timestamp']]
    df_youtube_channels.rename(columns={'subscriber_count': 'yt_subscribers', 'view_count': 'yt_total_views', 'video_count': 'yt_video_count'}, inplace=True)

    # Merge artist summary data
    # Use artist_id for merging to ensure correct join across platforms
    df_cleaned_artists = pd.merge(df_spotify_artists, df_youtube_channels, on=['artist_name', 'artist_id', 'collection_timestamp'], how='outer')

    # Convert numeric columns, fillna with 0 for counts/scores
    numeric_cols = ['spotify_followers', 'spotify_popularity', 'yt_subscribers', 'yt_total_views', 'yt_video_count']
    for col in numeric_cols:
        df_cleaned_artists[col] = pd.to_numeric(df_cleaned_artists[col], errors='coerce').fillna(0).astype(int)

    # Handle genres: convert list to string or extract primary genre
    df_cleaned_artists['genres'] = df_cleaned_artists['genres'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')


    # --- Clean Content Data (YouTube Videos & Spotify Tracks) ---
    df_cleaned_content = df_content_raw.copy()

    # Convert datetime columns
    df_cleaned_content['collection_timestamp'] = pd.to_datetime(df_cleaned_content['collection_timestamp'])
    # For YouTube: 'published_at' is ISO format
    df_cleaned_content.loc[df_cleaned_content['data_type'] == 'youtube_video', 'published_at'] = \
        pd.to_datetime(df_cleaned_content[df_cleaned_content['data_type'] == 'youtube_video']['published_at'], errors='coerce')
    # For Spotify: 'release_date' can be YYYY-MM-DD, YYYY-MM, or YYYY. Parse to YYYY-MM-DD.
    def parse_spotify_release_date(date_str):
        if pd.isna(date_str):
            return pd.NaT
        try:
            return pd.to_datetime(date_str)
        except ValueError:
            try: # Try partial date format
                return pd.to_datetime(date_str + '-01') # Assume 1st of month/year
            except ValueError:
                return pd.NaT
    df_cleaned_content.loc[df_cleaned_content['data_type'] == 'spotify_track', 'release_date'] = \
        df_cleaned_content[df_cleaned_content['data_type'] == 'spotify_track']['release_date'].apply(parse_spotify_release_date)


    # Convert numeric content columns, fillna with 0
    content_numeric_cols = ['view_count', 'like_count', 'comment_count', 'track_popularity']
    for col in content_numeric_cols:
        df_cleaned_content[col] = pd.to_numeric(df_cleaned_content[col], errors='coerce').fillna(0).astype(int)

    # Calculate YouTube engagement rate
    df_cleaned_content.loc[df_cleaned_content['data_type'] == 'youtube_video', 'engagement_rate'] = \
        df_cleaned_content[df_cleaned_content['data_type'] == 'youtube_video'].apply(
            lambda row: (row['like_count'] + row['comment_count']) / row['view_count'] if row['view_count'] > 0 else 0,
            axis=1
        )
    df_cleaned_content['engagement_rate'].fillna(0, inplace=True) # Fillna for spotify tracks or 0 view videos

    # Sentiment ratios are already floats, fillna with 0
    sentiment_cols = ['sentiment_positive_ratio', 'sentiment_negative_ratio', 'sentiment_neutral_ratio']
    for col in sentiment_cols:
        df_cleaned_content[col] = df_cleaned_content[col].fillna(0)

    # Drop any rows where artist_id is missing (shouldn't happen with our collection, but good safeguard)
    df_cleaned_artists.dropna(subset=['artist_id'], inplace=True)
    df_cleaned_content.dropna(subset=['artist_id'], inplace=True)

    print("Data cleaning and processing complete.")
    return df_cleaned_artists, df_cleaned_content

def save_processed_data(df_cleaned_artists, df_cleaned_content):
    """
    Saves the cleaned DataFrames to the processed data directory.
    """
    processed_artists_filepath = os.path.join(PROCESSED_DATA_DIR, 'cleaned_artists_data.csv')
    processed_content_filepath = os.path.join(PROCESSED_DATA_DIR, 'cleaned_content_data.csv')

    df_cleaned_artists.to_csv(processed_artists_filepath, index=False)
    df_cleaned_content.to_csv(processed_content_filepath, index=False)
    print(f"Cleaned artist data saved to: {processed_artists_filepath}")
    print(f"Cleaned content data saved to: {processed_content_filepath}")

if __name__ == "__main__":
    df_artists_raw, df_content_raw = load_raw_data()

    if not df_artists_raw.empty and not df_content_raw.empty:
        df_cleaned_artists, df_cleaned_content = clean_and_process_data(df_artists_raw, df_content_raw)
        save_processed_data(df_cleaned_artists, df_cleaned_content)
    else:
        print("Skipping cleaning due to no raw data.")
