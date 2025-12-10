# src/run_analysis.py
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime

# --- Configuration ---
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data/processed')
RESULTS_DIR = os.path.join(os.path.dirname(__file__), '../results')
os.makedirs(RESULTS_DIR, exist_ok=True)

CLEANED_ARTISTS_CSV = os.path.join(PROCESSED_DATA_DIR, 'cleaned_artists_data.csv')
CLEANED_CONTENT_CSV = os.path.join(PROCESSED_DATA_DIR, 'cleaned_content_data.csv')


# ----------------------
# Helper utilities
# ----------------------
def first_existing_column(df, candidates, default=None):
    """Return first column name in candidates that exists in df, otherwise default."""
    for c in candidates:
        if c in df.columns:
            return c
    return default

def safe_to_datetime(series):
    """Convert series to datetime safely (returns series of datetimes or NaT)."""
    return pd.to_datetime(series, errors='coerce')

def normalize_columns(df, cols):
    """Min-Max normalize specified columns and return new df with 'norm_' prefixed columns.
       If a column is constant or missing, create norm column filled with 0.0"""
    df_out = df.copy()
    scaler = MinMaxScaler()
    for col in cols:
        if col in df_out.columns and not df_out[col].isnull().all() and df_out[col].nunique(dropna=True) > 1:
            vals = df_out[[col]].fillna(0).astype(float)
            df_out[f'norm_{col}'] = scaler.fit_transform(vals)
        else:
            # column missing or constant -> set normalized to 0.0
            df_out[f'norm_{col}'] = 0.0
    return df_out


# ----------------------
# Load cleaned data
# ----------------------
def load_cleaned_data():
    if not os.path.exists(CLEANED_ARTISTS_CSV) or not os.path.exists(CLEANED_CONTENT_CSV):
        print("Cleaned data files not found. Please run clean_data.py first and ensure CSVs exist:")
        print(f"  - {CLEANED_ARTISTS_CSV}")
        print(f"  - {CLEANED_CONTENT_CSV}")
        return pd.DataFrame(), pd.DataFrame()

    df_artists = pd.read_csv(CLEANED_ARTISTS_CSV)
    df_content = pd.read_csv(CLEANED_CONTENT_CSV)

    # Normalize column names (some pipelines may call things differently)
    # Common expected artist columns: 'artist_name' (identifier), 'tiktok_views', 'tiktok_video_count',
    # 'yt_subscribers' or 'subscriber_count', 'yt_total_views' or 'view_count' (artist-level)
    # Common content columns (videos/tracks): 'view_count','like_count','comment_count','published_at'

    # Convert possible datetime columns
    if 'collection_timestamp' in df_artists.columns:
        df_artists['collection_timestamp'] = safe_to_datetime(df_artists['collection_timestamp'])
    if 'collection_timestamp' in df_content.columns:
        df_content['collection_timestamp'] = safe_to_datetime(df_content['collection_timestamp'])
    if 'published_at' in df_content.columns:
        df_content['published_at'] = safe_to_datetime(df_content['published_at'])
    if 'release_date' in df_content.columns:
        df_content['release_date'] = safe_to_datetime(df_content['release_date'])

    return df_artists, df_content


# ----------------------
# Main analysis function
# ----------------------
def perform_analysis(df_artists, df_content):
    """
    1) Compute YouTube engagement rates from content-level data
    2) Aggregate per-artist metrics (avg engagement, sentiment)
    3) Normalize metrics and compute Combined Popularity Index using Option 2 weights
    4) Temporal analysis for YouTube content lifespan
    5) Save analyzed CSVs
    """

    print("Starting analysis...")

    if df_artists.empty or df_content.empty:
        print("Empty input dataframes — aborting analysis.")
        return None

    # Use 'artist_name' as primary id; if not present, try 'artist_id' or 'artist'
    if 'artist_name' not in df_artists.columns:
        if 'artist_id' in df_artists.columns:
            df_artists.rename(columns={'artist_id': 'artist_name'}, inplace=True)
        elif 'artist' in df_artists.columns:
            df_artists.rename(columns={'artist': 'artist_name'}, inplace=True)
        else:
            # As last resort, create artist_name from file if possible — but prefer the cleaned pipeline to provide it
            print("Warning: 'artist_name' not found in artist file. Ensure cleaned_artists_data.csv has artist_name column.")
    
    # Ensure content has artist_name too
    if 'artist_name' not in df_content.columns:
        if 'artist_id' in df_content.columns:
            df_content.rename(columns={'artist_id': 'artist_name'}, inplace=True)
        elif 'artist' in df_content.columns:
            df_content.rename(columns={'artist': 'artist_name'}, inplace=True)

    # 1) Compute engagement_rate for YouTube videos in content-level data
    # Engagement rate defined as (like_count + comment_count) / view_count
    # Use safe columns names
    view_col = first_existing_column(df_content, ['view_count', 'views', 'yt_view_count'])
    like_col = first_existing_column(df_content, ['like_count', 'likes', 'yt_like_count'])
    comment_col = first_existing_column(df_content, ['comment_count', 'comments', 'yt_comment_count'])

    df_content = df_content.copy()
    if view_col:
        df_content['view_count_safe'] = pd.to_numeric(df_content[view_col], errors='coerce').fillna(0).astype(float)
    else:
        df_content['view_count_safe'] = 0.0

    if like_col:
        df_content['like_count_safe'] = pd.to_numeric(df_content[like_col], errors='coerce').fillna(0).astype(float)
    else:
        df_content['like_count_safe'] = 0.0

    if comment_col:
        df_content['comment_count_safe'] = pd.to_numeric(df_content[comment_col], errors='coerce').fillna(0).astype(float)
    else:
        df_content['comment_count_safe'] = 0.0

    # engagement rate (protect div by zero)
    df_content['engagement_rate'] = 0.0
    mask = df_content['view_count_safe'] > 0
    df_content.loc[mask, 'engagement_rate'] = (
        (df_content.loc[mask, 'like_count_safe'] + df_content.loc[mask, 'comment_count_safe']) /
        df_content.loc[mask, 'view_count_safe']
    )

    # 2) Aggregate per-artist YouTube metrics
    yt_videos = df_content[df_content.get('data_type', '') == 'youtube_video'] if 'data_type' in df_content.columns else df_content
    if 'data_type' in df_content.columns:
        yt_videos = df_content[df_content['data_type'] == 'youtube_video']
    else:
        # If no data_type, try to detect youtube rows by presence of published_at or view_count
        yt_videos = df_content[df_content['view_count_safe'] > 0]

    # Average engagement and sentiment per artist
    avg_yt_engagement = yt_videos.groupby('artist_name')['engagement_rate'].mean().reset_index().rename(columns={'engagement_rate':'avg_yt_engagement_rate'})

    # Sentiment: average of sentiment_positive_ratio if present
    sentiment_col = first_existing_column(yt_videos, ['sentiment_positive_ratio','sentiment_positive','pos_sentiment'])
    if sentiment_col:
        avg_sent = yt_videos.groupby('artist_name')[[sentiment_col]].mean().reset_index().rename(columns={sentiment_col:'avg_sentiment_positive'})
    else:
        avg_sent = pd.DataFrame({'artist_name': df_artists['artist_name'], 'avg_sentiment_positive': 0.0})

    # Merge these into artist-level df
    df_artists = pd.merge(df_artists, avg_yt_engagement, on='artist_name', how='left')
    df_artists = pd.merge(df_artists, avg_sent, on='artist_name', how='left')
    df_artists['avg_yt_engagement_rate'] = df_artists['avg_yt_engagement_rate'].fillna(0.0)
    if 'avg_sentiment_positive' in df_artists.columns:
        df_artists['avg_sentiment_positive'] = df_artists['avg_sentiment_positive'].fillna(0.0)
    else:
        df_artists['avg_sentiment_positive'] = 0.0

    # 3) Find artist-level YouTube subscriber and total view columns (artist summary file)
    # Common names: 'yt_subscribers', 'subscriber_count', 'channel_subscribers'
    yt_sub_col = first_existing_column(df_artists, ['yt_subscribers','subscriber_count','channel_subscribers','subscribers'])
    yt_views_col = first_existing_column(df_artists, ['yt_total_views','view_count','channel_view_count','total_views'])

    # Convert to numeric safely
    if yt_sub_col:
        df_artists['yt_subscribers_numeric'] = pd.to_numeric(df_artists[yt_sub_col], errors='coerce').fillna(0).astype(float)
    else:
        df_artists['yt_subscribers_numeric'] = 0.0

    if yt_views_col:
        df_artists['yt_total_views_numeric'] = pd.to_numeric(df_artists[yt_views_col], errors='coerce').fillna(0).astype(float)
    else:
        df_artists['yt_total_views_numeric'] = 0.0

    # 4) Find TikTok metrics in artist-level df (tiktok_views, tiktok_video_count)
    tik_views_col = first_existing_column(df_artists, ['tiktok_views','tiktok_total_views','tk_views'])
    tik_vcount_col = first_existing_column(df_artists, ['tiktok_video_count','tiktok_videos','tk_video_count'])

    if tik_views_col:
        df_artists['tiktok_views_numeric'] = pd.to_numeric(df_artists[tik_views_col], errors='coerce').fillna(0).astype(float)
    else:
        df_artists['tiktok_views_numeric'] = 0.0

    if tik_vcount_col:
        df_artists['tiktok_video_count_numeric'] = pd.to_numeric(df_artists[tik_vcount_col], errors='coerce').fillna(0).astype(float)
    else:
        df_artists['tiktok_video_count_numeric'] = 0.0

    # 5) Build normalized df using Min-Max scaling for chosen metrics
    metrics_to_norm = [
        'yt_subscribers_numeric',
        'yt_total_views_numeric',
        'avg_yt_engagement_rate',
        'tiktok_views_numeric',
        'tiktok_video_count_numeric'
    ]

    df_artists_norm = normalize_columns(df_artists, metrics_to_norm)

    # 6) Combined Popularity Index (Option 2 weights)
    weights = {
        'yt_engagement_rate': 0.35,
        'yt_total_views': 0.20,
        'yt_subscribers': 0.15,
        'tiktok_views': 0.20,
        'tiktok_video_count': 0.10
    }

    df_artists_norm['combined_popularity_index'] = (
        weights['yt_engagement_rate'] * df_artists_norm['norm_avg_yt_engagement_rate'] +
        weights['yt_total_views'] * df_artists_norm['norm_yt_total_views_numeric'] +
        weights['yt_subscribers'] * df_artists_norm['norm_yt_subscribers_numeric'] +
        weights['tiktok_views'] * df_artists_norm['norm_tiktok_views_numeric'] +
        weights['tiktok_video_count'] * df_artists_norm['norm_tiktok_video_count_numeric']
    )

    # 7) Temporal analysis for YouTube content: age categories & average performance
    if 'collection_timestamp' in df_content.columns:
        collection_date = df_content['collection_timestamp'].dropna().iloc[0] if not df_content['collection_timestamp'].dropna().empty else pd.Timestamp.now()
    else:
        collection_date = pd.Timestamp.now()

    # compute age_days for youtube videos
    df_content_age = df_content.copy()
    if 'published_at' in df_content_age.columns:
        df_content_age['published_at_dt'] = safe_to_datetime(df_content_age['published_at'])
        df_content_age['age_days'] = (pd.to_datetime(collection_date) - df_content_age['published_at_dt']).dt.days
    else:
        df_content_age['age_days'] = np.nan

    def categorize_age(days):
        if pd.isna(days):
            return 'Unknown'
        if days < 0:
            return 'Future'
        if days <= 30: return '0-1 Month'
        if days <= 90: return '1-3 Months'
        if days <= 365: return '3-12 Months'
        if days <= 365 * 3: return '1-3 Years'
        return '3+ Years'

    df_content_age['age_category'] = df_content_age['age_days'].apply(categorize_age)

    # Filter to youtube videos only (if data_type present)
    if 'data_type' in df_content_age.columns:
        yt_content_age = df_content_age[df_content_age['data_type'] == 'youtube_video'].copy()
    else:
        yt_content_age = df_content_age[df_content_age['view_count_safe'] > 0].copy()

    # aggregate performance per artist / age_category
    if not yt_content_age.empty:
        yt_age_perf = yt_content_age.groupby(['artist_name', 'age_category']).agg(
            avg_views=('view_count_safe', 'mean'),
            avg_engagement=('engagement_rate', 'mean'),
            num_videos=('video_id' if 'video_id' in yt_content_age.columns else 'view_count_safe', 'count')
        ).reset_index()
    else:
        yt_age_perf = pd.DataFrame(columns=['artist_name','age_category','avg_views','avg_engagement','num_videos'])

    # 8) Release frequency estimate: videos per month for YouTube using published_at span
    release_list = []
    for artist in df_artists_norm['artist_name'].unique():
        artist_videos = yt_content_age[yt_content_age['artist_name'] == artist]
        published = artist_videos['published_at_dt'].dropna() if 'published_at_dt' in artist_videos.columns else pd.Series([], dtype='datetime64[ns]')
        if not published.empty:
            earliest = published.min()
            latest = published.max()
            span_months = max( (latest - earliest).days / 30.4375, 1.0)
            avg_videos_per_month = len(published) / span_months
        else:
            avg_videos_per_month = 0.0
        release_list.append({'artist_name': artist, 'avg_yt_videos_per_month': avg_videos_per_month})
    df_release_freq = pd.DataFrame(release_list)

    df_artists_final = pd.merge(df_artists_norm, df_release_freq, on='artist_name', how='left')
    df_artists_final['avg_yt_videos_per_month'] = df_artists_final['avg_yt_videos_per_month'].fillna(0.0)

    # 9) Genre or category insights (if 'genres' column exists)
    if 'genres' in df_artists_final.columns:
        # assume genres stored as a list-like string, take first as primary
        df_artists_final['primary_genre'] = df_artists_final['genres'].fillna('Unknown').apply(lambda x: x.split(',')[0] if isinstance(x, str) else 'Unknown')
    else:
        df_artists_final['primary_genre'] = 'Unknown'

    genre_metrics = df_artists_final.groupby('primary_genre').agg(
        avg_popularity_index=('combined_popularity_index', 'mean'),
        avg_yt_subscribers=('norm_yt_subscribers_numeric', 'mean'),
        avg_tiktok_views=('norm_tiktok_views_numeric', 'mean'),
        num_artists=('artist_name', 'count')
    ).reset_index()

    # ------------------------------
    # Save outputs back to processed dir
    # ------------------------------
    analyzed_artists_fp = os.path.join(PROCESSED_DATA_DIR, 'analyzed_artists_data.csv')
    yt_age_perf_fp = os.path.join(PROCESSED_DATA_DIR, 'analyzed_yt_age_performance.csv')
    genre_metrics_fp = os.path.join(PROCESSED_DATA_DIR, 'analyzed_genre_metrics.csv')

    df_artists_final.to_csv(analyzed_artists_fp, index=False)
    yt_age_perf.to_csv(yt_age_perf_fp, index=False)
    genre_metrics.to_csv(genre_metrics_fp, index=False)

    print(f"Saved analyzed artists data -> {analyzed_artists_fp}")
    print(f"Saved YouTube age performance -> {yt_age_perf_fp}")
    print(f"Saved genre metrics -> {genre_metrics_fp}")

    print("\nTop artists by combined_popularity_index:")
    display_cols = ['artist_name', 'combined_popularity_index', 'primary_genre', 'avg_yt_videos_per_month']
    if 'combined_popularity_index' in df_artists_final.columns:
        print(df_artists_final[display_cols].sort_values('combined_popularity_index', ascending=False).head(10).to_string(index=False))
    else:
        print("No combined index computed.")

    return df_artists_final, yt_age_perf, genre_metrics


# ----------------------
# Run as script
# ----------------------
if __name__ == "__main__":
    df_artists, df_content = load_cleaned_data()
    if df_artists.empty or df_content.empty:
        print("No cleaned data, aborting.")
    else:
        df_artists_final, yt_age_perf, genre_metrics = perform_analysis(df_artists, df_content)
        print("\nAnalysis complete.")
