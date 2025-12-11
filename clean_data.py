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
    Loads the most recent raw CSV data file from data/raw.
    Assumes file names are in the format:
    artists_summary_YYYYMMDD_HHMMSS.csv
    """
    # Find the most recent snapshot file
    artist_files = sorted([f for f in os.listdir(RAW_DATA_DIR) if f.startswith('artists_summary_') and f.endswith('.csv')], reverse=True)

    if not artist_files:
        print("No raw data files found. Please run get_data.py first.")
        return pd.DataFrame()

    most_recent_file = os.path.join(RAW_DATA_DIR, artist_files[0])
    print(f"Loading raw artists data from: {most_recent_file}")
    
    # Load CSV directly
    df_artists = pd.read_csv(most_recent_file)
    return df_artists

def clean_and_process_data(df_artists):
    """
    Cleans, normalizes, and calculates popularity index.
    """
    print("Starting data cleaning and processing...")

    # Create a copy to avoid SettingWithCopy warnings
    df_cleaned = df_artists.copy()

    # 1. Handle Missing Values (fillna with 0 for counts)
    numeric_cols = ['tiktok_post_count', 'youtube_total_views', 'youtube_subs']
    for col in numeric_cols:
        if col in df_cleaned.columns:
            df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce').fillna(0).astype(int)

    # 2. Normalization (Min-Max Scaling)
    # We implement Min-Max scaling manually to avoid dependency on sklearn for this simple step
    # Formula: (x - min) / (max - min)
    
    # Normalize YouTube Views
    yt_min = df_cleaned['youtube_total_views'].min()
    yt_max = df_cleaned['youtube_total_views'].max()
    df_cleaned['norm_youtube'] = (df_cleaned['youtube_total_views'] - yt_min) / (yt_max - yt_min)

    # Normalize TikTok Posts
    tt_min = df_cleaned['tiktok_post_count'].min()
    tt_max = df_cleaned['tiktok_post_count'].max()
    # Avoid division by zero if max == min
    if tt_max > tt_min:
        df_cleaned['norm_tiktok'] = (df_cleaned['tiktok_post_count'] - tt_min) / (tt_max - tt_min)
    else:
        df_cleaned['norm_tiktok'] = 0

    # 3. Calculate Popularity Index
    # Methodology: 55% weight to YouTube (long-term stats), 45% to TikTok (viral activity)
    df_cleaned['popularity_index'] = (0.55 * df_cleaned['norm_youtube']) + (0.45 * df_cleaned['norm_tiktok'])
    
    # Scale to 0-100 for readability
    df_cleaned['popularity_score_100'] = (df_cleaned['popularity_index'] * 100).round(2)

    # 4. Rank Artists
    df_cleaned = df_cleaned.sort_values('popularity_index', ascending=False).reset_index(drop=True)
    df_cleaned['rank'] = df_cleaned.index + 1

    print("Data cleaning and processing complete.")
    return df_cleaned

def save_processed_data(df_cleaned):
    """
    Saves the cleaned DataFrame to the processed data directory.
    """
    processed_filepath = os.path.join(PROCESSED_DATA_DIR, 'final_ranked_artists.csv')
    df_cleaned.to_csv(processed_filepath, index=False)
    print(f"Cleaned and ranked data saved to: {processed_filepath}")
    
    # Also print the top 5 for verification
    print("\n--- Top 5 Artists ---")
    print(df_cleaned[['rank', 'artist', 'popularity_score_100', 'youtube_total_views', 'tiktok_post_count']].head(5))

if __name__ == "__main__":
    df_raw = load_raw_data()

    if not df_raw.empty:
        df_processed = clean_and_process_data(df_raw)
        save_processed_data(df_processed)
    else:
        print("Skipping cleaning due to no raw data.")
