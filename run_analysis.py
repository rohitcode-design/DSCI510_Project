# src/run_analysis.py
import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime, timedelta

# --- Configuration ---
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data/processed')
RESULTS_DIR = os.path.join(os.path.dirname(__file__), '../results') # Not strictly needed here, but good practice
os.makedirs(RESULTS_DIR, exist_ok=True) # Ensure results directory exists

def load_cleaned_data():
    """Loads the cleaned CSV data from data/processed."""
    cleaned_artists_filepath = os.path.join(PROCESSED_DATA_DIR, 'cleaned_artists_data.csv')
    cleaned_content_filepath = os.path.join(PROCESSED_DATA_DIR, 'cleaned_content_data.csv')

    if not os.path.exists(cleaned_artists_filepath) or not os.path.exists(cleaned_content_filepath):
        print("Cleaned data files not found. Please run clean_data.py first.")
        return pd.DataFrame(), pd.DataFrame()

    df_artists = pd.read_csv(cleaned_artists_filepath)
    df_content = pd.read_csv(cleaned_content_filepath)

    # Convert datetime columns back to datetime objects
    df_artists['collection_timestamp'] = pd.to_datetime(df_artists['collection_timestamp'])
    df_content['collection_timestamp'] = pd.to_datetime(df_content['collection_timestamp'])
    df_content['published_at'] = pd.to_datetime(df_content['published_at'])
    df_content['release_date'] = pd.to_datetime(df_content['release_date'])

    return df_artists, df_content

def normalize_columns(df, columns_to_normalize):
    """Applies Min-Max scaling to specified columns in a DataFrame."""
    scaler = MinMaxScaler()
    df_normalized = df.copy()
    for col in columns_to_normalize:
        if col in df.columns and not df[col].isnull().all() and df[col].nunique() > 1:
            df_normalized[f'norm_{col}'] = scaler.fit_transform(df[[col]])
        else: # Handle cases where column is all same value or all NaN
            df_normalized[f'norm_{col}'] = 0.0 # Or 0.5 if it's considered 'average'
    return df_normalized

def perform_analysis(df_artists, df_content):
    """
    Performs normalization, calculates combined popularity index,
    and conducts temporal and sentiment analysis.
    """
    print("Starting data analysis...")

    # --- 1. Normalization ---
    # First, calculate average video engagement and sentiment for each artist
    df_youtube_videos = df_content[df_content['data_type'] == 'youtube_video'].copy()

    # Ensure no division by zero for engagement rate calculation if needed later
    df_youtube_videos.loc[df_youtube_videos['view_count'] == 0, 'engagement_rate'] = 0

    avg_yt_engagement = df_youtube_videos.groupby('artist_id')['engagement_rate'].mean().reset_index()
    avg_yt_engagement.rename(columns={'engagement_rate': 'avg_yt_engagement_rate'}, inplace=True)

    avg_yt_sentiment = df_youtube_videos.groupby('artist_id')[['sentiment_positive_ratio', 'sentiment_negative_ratio', 'sentiment_neutral_ratio']].mean().reset_index()

    # Merge these back into df_artists
    df_artists = pd.merge(df_artists, avg_yt_engagement, on='artist_id', how='left')
    df_artists = pd.merge(df_artists, avg_yt_sentiment, on='artist_id', how='left')
    # Fill NaN for artists with no YouTube videos
    df_artists['avg_yt_engagement_rate'] = df_artists['avg_yt_engagement_rate'].fillna(0)
    df_artists[['sentiment_positive_ratio', 'sentiment_negative_ratio', 'sentiment_neutral_ratio']] = \
        df_artists[['sentiment_positive_ratio', 'sentiment_negative_ratio', 'sentiment_neutral_ratio']].fillna(0)


    # Normalize key metrics for artists
    artist_metrics_to_normalize = [
        'spotify_followers',
        'spotify_popularity',
        'yt_subscribers',
        'yt_total_views',
        'avg_yt_engagement_rate' # Normalized average engagement rate
    ]
    df_artists_normalized = normalize_columns(df_artists, artist_metrics_to_normalize)
    
    # --- 2. Combined Popularity Index ---
    weights = {
        'spotify_popularity': 0.25,
        'spotify_followers': 0.15,
        'yt_subscribers': 0.15,
        'yt_total_views': 0.25,
        'avg_yt_engagement_rate': 0.20
    }

    # Calculate index using normalized values
    df_artists_normalized['combined_popularity_index'] = (
        weights['spotify_popularity'] * df_artists_normalized['norm_spotify_popularity'] +
        weights['spotify_followers'] * df_artists_normalized['norm_spotify_followers'] +
        weights['yt_subscribers'] * df_artists_normalized['norm_yt_subscribers'] +
        weights['yt_total_views'] * df_artists_normalized['norm_yt_total_views'] +
        weights['avg_yt_engagement_rate'] * df_artists_normalized['norm_avg_yt_engagement_rate']
    )

    # --- 3. Temporal Analysis (Revised Focus) ---
    # a. Content Lifespan Performance
    df_content_with_age = df_content.copy()
    collection_date = df_content_with_age['collection_timestamp'].iloc[0] # All collected at same time

    # Calculate age of content at time of collection
    df_content_with_age.loc[df_content_with_age['data_type'] == 'youtube_video', 'age_days'] = \
        (collection_date - df_content_with_age[df_content_with_age['data_type'] == 'youtube_video']['published_at']).dt.days

    df_content_with_age.loc[df_content_with_age['data_type'] == 'spotify_track', 'age_days'] = \
        (collection_date - df_content_with_age[df_content_with_age['data_type'] == 'spotify_track']['release_date']).dt.days

    # Categorize content age
    def categorize_age(days):
        if pd.isna(days) or days < 0: # Handle future dates or missing
            return 'Unknown'
        if days <= 30: return '0-1 Month'
        if days <= 90: return '1-3 Months'
        if days <= 365: return '3-12 Months'
        if days <= 365 * 3: return '1-3 Years'
        return '3+ Years'

    df_content_with_age['age_category'] = df_content_with_age['age_days'].apply(categorize_age)
    df_content_with_age = df_content_with_age[df_content_with_age['age_category'] != 'Unknown'].copy() # Remove unknown ages

    # Aggregate performance by age category per artist
    # For YouTube videos: average views and engagement
    avg_yt_performance_by_age = df_content_with_age[df_content_with_age['data_type'] == 'youtube_video'].groupby(['artist_id', 'age_category']).agg(
        avg_views=('view_count', 'mean'),
        avg_engagement=('engagement_rate', 'mean'),
        num_videos=('video_id', 'count')
    ).reset_index()

    # For Spotify tracks: average track popularity
    avg_spotify_performance_by_age = df_content_with_age[df_content_with_age['data_type'] == 'spotify_track'].groupby(['artist_id', 'age_category']).agg(
        avg_track_popularity=('track_popularity', 'mean'),
        num_tracks=('track_id', 'count')
    ).reset_index()


    # b. Release Frequency & Popularity (simple approximation)
    release_frequency_data = []
    for artist_id in df_artists['artist_id'].unique():
        artist_content = df_content_with_age[df_content_with_age['artist_id'] == artist_id]
        
        # Count unique release months for YouTube videos
        yt_releases = artist_content[artist_content['data_type'] == 'youtube_video']['published_at'].dropna()
        num_yt_months = len(yt_releases.dt.to_period('M').unique())
        num_yt_videos = len(yt_releases)

        # Count unique release months for Spotify tracks
        sp_releases = artist_content[artist_content['data_type'] == 'spotify_track']['release_date'].dropna()
        num_sp_months = len(sp_releases.dt.to_period('M').unique())
        num_sp_tracks = len(sp_releases)
        
        # Calculate average content per month over the collected content's span
        # This is an approximation as we don't have *all* content for *all* time
        earliest_yt = yt_releases.min() if not yt_releases.empty else pd.NaT
        latest_yt = yt_releases.max() if not yt_releases.empty else pd.NaT
        yt_span_months = ((latest_yt - earliest_yt).days / 30.4375) if not pd.isna(earliest_yt) and not pd.isna(latest_yt) else 0
        avg_yt_content_per_month = num_yt_videos / yt_span_months if yt_span_months > 0 else 0

        earliest_sp = sp_releases.min() if not sp_releases.empty else pd.NaT
        latest_sp = sp_releases.max() if not sp_releases.empty else pd.NaT
        sp_span_months = ((latest_sp - earliest_sp).days / 30.4375) if not pd.isna(earliest_sp) and not pd.isna(latest_sp) else 0
        avg_sp_content_per_month = num_sp_tracks / sp_span_months if sp_span_months > 0 else 0

        release_frequency_data.append({
            'artist_id': artist_id,
            'avg_yt_content_per_month': avg_yt_content_per_month,
            'avg_sp_content_per_month': avg_sp_content_per_month,
            'total_content_count': num_yt_videos + num_sp_tracks # Total number of content items we fetched
        })
    df_release_frequency = pd.DataFrame(release_frequency_data)
    df_artists_normalized = pd.merge(df_artists_normalized, df_release_frequency, on='artist_id', how='left')
    df_artists_normalized[['avg_yt_content_per_month', 'avg_sp_content_per_month', 'total_content_count']] = \
        df_artists_normalized[['avg_yt_content_per_month', 'avg_sp_content_per_month', 'total_content_count']].fillna(0)


    # --- 4. Genre-Specific Insights (Aggregating to genre level) ---
    # First, add the primary genre to the main artist dataframe for easier grouping
    # For now, let's just use the first genre listed for simplicity
    df_artists_normalized['primary_genre'] = df_artists_normalized['genres'].apply(lambda x: x.split(', ')[0] if pd.notna(x) and x else 'Unknown')
    
    genre_metrics = df_artists_normalized.groupby('primary_genre').agg(
        avg_popularity_index=('combined_popularity_index', 'mean'),
        avg_spotify_pop=('norm_spotify_popularity', 'mean'),
        avg_yt_subscribers=('norm_yt_subscribers', 'mean'),
        num_artists=('artist_id', 'count')
    ).reset_index()


    print("Data analysis complete.")
    return df_artists_normalized, avg_yt_performance_by_age, avg_spotify_performance_by_age, genre_metrics

if __name__ == "__main__":
    df_artists, df_content = load_cleaned_data()

    if not df_artists.empty and not df_content.empty:
        df_analyzed_artists, df_yt_age_performance, df_sp_age_performance, df_genre_metrics = perform_analysis(df_artists, df_content)

        # You might want to save these analyzed dataframes as well for easier visualization
        df_analyzed_artists.to_csv(os.path.join(PROCESSED_DATA_DIR, 'analyzed_artists_data.csv'), index=False)
        df_yt_age_performance.to_csv(os.path.join(PROCESSED_DATA_DIR, 'analyzed_yt_age_performance.csv'), index=False)
        df_sp_age_performance.to_csv(os.path.join(PROCESSED_DATA_DIR, 'analyzed_sp_age_performance.csv'), index=False)
        df_genre_metrics.to_csv(os.path.join(PROCESSED_DATA_DIR, 'analyzed_genre_metrics.csv'), index=False)

        print("\n--- Analyzed Artist Data Sample ---")
        print(df_analyzed_artists[['artist_name', 'combined_popularity_index', 'primary_genre', 'norm_spotify_popularity', 'norm_yt_subscribers', 'avg_yt_content_per_month']].sort_values(by='combined_popularity_index', ascending=False).head())
        
        print("\n--- YouTube Performance by Age Category Sample ---")
        print(df_yt_age_performance.head())

        print("\n--- Spotify Performance by Age Category Sample ---")
        print(df_sp_age_performance.head())

        print("\n--- Genre Metrics Sample ---")
        print(df_genre_metrics.head())
    else:
        print("Skipping analysis due to no cleaned data.")
