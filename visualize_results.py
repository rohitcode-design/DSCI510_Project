# src/visualize_results.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

# --- Configuration ---
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data/processed')
RESULTS_DIR = os.path.join(os.path.dirname(__file__), '../results')
os.makedirs(RESULTS_DIR, exist_ok=True)  # Ensure results directory exists

ANALYZED_ARTISTS_FP = os.path.join(PROCESSED_DATA_DIR, 'analyzed_artists_data.csv')
ANALYZED_YT_AGE_FP = os.path.join(PROCESSED_DATA_DIR, 'analyzed_yt_age_performance.csv')
ANALYZED_GENRE_FP = os.path.join(PROCESSED_DATA_DIR, 'analyzed_genre_metrics.csv')


def load_analyzed_data():
    """Loads the analyzed CSV data from data/processed. Returns dataframes or None if missing."""
    missing = []
    if not os.path.exists(ANALYZED_ARTISTS_FP):
        missing.append(ANALYZED_ARTISTS_FP)
    if not os.path.exists(ANALYZED_YT_AGE_FP):
        missing.append(ANALYZED_YT_AGE_FP)
    if not os.path.exists(ANALYZED_GENRE_FP):
        missing.append(ANALYZED_GENRE_FP)

    if missing:
        print("Warning: The following analyzed files are missing (some visualizations will be skipped):")
        for m in missing:
            print("  -", m)

    df_artists = pd.read_csv(ANALYZED_ARTISTS_FP) if os.path.exists(ANALYZED_ARTISTS_FP) else pd.DataFrame()
    df_yt_age = pd.read_csv(ANALYZED_YT_AGE_FP) if os.path.exists(ANALYZED_YT_AGE_FP) else pd.DataFrame()
    df_genre = pd.read_csv(ANALYZED_GENRE_FP) if os.path.exists(ANALYZED_GENRE_FP) else pd.DataFrame()

    return df_artists, df_yt_age, df_genre


def safe_savefig(fig_name):
    """Helper to save figures to RESULTS_DIR with a printout."""
    path = os.path.join(RESULTS_DIR, fig_name)
    plt.savefig(path, bbox_inches='tight')
    print("Generated:", fig_name)


def create_visualizations(df_artists, df_yt_age, df_genre):
    """Generates and saves visualizations (YouTube + TikTok focused)."""
    print("Starting visualization generation...")
    sns.set_theme(style="whitegrid")

    # ---------- 1) Combined Popularity Index Bar Chart ----------
    if not df_artists.empty and 'combined_popularity_index' in df_artists.columns:
        plt.figure(figsize=(12, 7))
        order = df_artists.sort_values(by='combined_popularity_index', ascending=False)['artist_name']
        sns.barplot(
            x='artist_name',
            y='combined_popularity_index',
            data=df_artists,
            order=order,
            palette='viridis'
        )
        plt.title('Combined Popularity Index by Artist', fontsize=16)
        plt.xlabel('Artist', fontsize=12)
        plt.ylabel('Combined Popularity Index (normalized)', fontsize=12)
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.tight_layout()
        safe_savefig('combined_popularity_index.png')
        plt.close()
    else:
        print("Skipping combined popularity index chart: data missing or column not present.")

    # ---------- 2) YouTube Avg Views by Age Category (grouped by artist) ----------
    if not df_yt_age.empty and {'age_category', 'avg_views', 'artist_name'}.issubset(df_yt_age.columns):
        # order categories if present
        age_order = ['0-1 Month', '1-3 Months', '3-12 Months', '1-3 Years', '3+ Years']
        df_yt_age['age_category'] = pd.Categorical(df_yt_age['age_category'], categories=age_order, ordered=True)

        plt.figure(figsize=(14, 8))
        sns.barplot(
            x='age_category',
            y='avg_views',
            hue='artist_name',
            data=df_yt_age.sort_values(by=['artist_name', 'age_category']),
            palette='deep'
        )
        plt.title('Average YouTube Video Views by Content Age and Artist', fontsize=16)
        plt.xlabel('Content Age Category', fontsize=12)
        plt.ylabel('Average Views', fontsize=12)
        plt.xticks(rotation=35, ha='right')
        plt.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        safe_savefig('yt_avg_views_by_age.png')
        plt.close()
    else:
        print("Skipping YouTube age-performance chart: required columns missing.")

    # ---------- 3) Release Frequency vs Popularity Scatter ----------
    # Choose a sensible x-axis: prefer 'avg_yt_videos_per_month' then 'total_content_count'
    if not df_artists.empty and 'combined_popularity_index' in df_artists.columns:
        x_col = None
        if 'avg_yt_videos_per_month' in df_artists.columns:
            x_col = 'avg_yt_videos_per_month'
            x_label = 'Average YouTube Videos per Month'
        elif 'total_content_count' in df_artists.columns:
            x_col = 'total_content_count'
            x_label = 'Total Content Items (fetched)'
        else:
            x_col = None

        if x_col:
            plt.figure(figsize=(10, 7))
            sns.scatterplot(
                x=x_col,
                y='combined_popularity_index',
                hue='artist_name',
                data=df_artists,
                s=180,
                alpha=0.8,
                palette='tab10'
            )
            plt.title('Release Frequency vs Combined Popularity Index', fontsize=16)
            plt.xlabel(x_label, fontsize=12)
            plt.ylabel('Combined Popularity Index', fontsize=12)
            plt.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            safe_savefig('release_frequency_vs_popularity.png')
            plt.close()
        else:
            print("Skipping release frequency scatter: no suitable frequency column found.")
    else:
        print("Skipping release frequency scatter: artist data or combined index missing.")

    # ---------- 4) TikTok vs YouTube: Scatter (if tiktok + yt columns present) ----------
    # We look for normalized tiktok and yt columns if present, else numeric raw columns
    if not df_artists.empty:
        tik_col = None
        ytv_col = None

        # prefer normalized columns if available
        if 'norm_tiktok_views_numeric' in df_artists.columns:
            tik_col = 'norm_tiktok_views_numeric'
            tik_label = 'Normalized TikTok Views'
        elif 'tiktok_views_numeric' in df_artists.columns:
            tik_col = 'tiktok_views_numeric'
            tik_label = 'TikTok Views (raw)'

        if 'norm_yt_subscribers_numeric' in df_artists.columns:
            ytv_col = 'norm_yt_subscribers_numeric'
            ytv_label = 'Normalized YouTube Subscribers'
        elif 'yt_subscribers_numeric' in df_artists.columns:
            ytv_col = 'yt_subscribers_numeric'
            ytv_label = 'YouTube Subscribers (raw)'

        if tik_col and ytv_col:
            plt.figure(figsize=(10, 7))
            sns.scatterplot(
                x=tik_col,
                y=ytv_col,
                hue='artist_name',
                data=df_artists,
                s=160,
                alpha=0.8,
                palette='tab10'
            )
            plt.title(f'{tik_label} vs {ytv_label}', fontsize=14)
            plt.xlabel(tik_label, fontsize=12)
            plt.ylabel(ytv_label, fontsize=12)
            plt.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            safe_savefig('tiktok_vs_youtube_subs.png')
            plt.close()
        else:
            print("Skipping TikTok vs YouTube scatter: required columns missing (TikTok or YouTube subscribers).")
    else:
        print("Skipping TikTok vs YouTube scatter: no artist data.")

    # ---------- 5) Sentiment Donut for top artist (if sentiment metrics exist) ----------
    # Look for sentiment columns or averaged columns
    if not df_artists.empty:
        pos_col = None
        neg_col = None
        neu_col = None
        for c in ['sentiment_positive_ratio', 'sentiment_positive_ratio_avg', 'avg_sentiment_positive', 'sentiment_positive']:
            if c in df_artists.columns:
                pos_col = c
                break
        for c in ['sentiment_negative_ratio', 'sentiment_negative_ratio_avg', 'sentiment_negative']:
            if c in df_artists.columns:
                neg_col = c
                break
        for c in ['sentiment_neutral_ratio', 'sentiment_neutral_ratio_avg', 'sentiment_neutral']:
            if c in df_artists.columns:
                neu_col = c
                break

        if pos_col or neg_col or neu_col:
            # pick top artist by combined index if present, else first row
            if 'combined_popularity_index' in df_artists.columns:
                example_artist = df_artists.sort_values('combined_popularity_index', ascending=False).iloc[0]
            else:
                example_artist = df_artists.iloc[0]

            artist_name = example_artist.get('artist_name', example_artist.get('artist', 'Unknown')).strip()
            pos = float(example_artist[pos_col]) if pos_col and pd.notna(example_artist[pos_col]) else 0.0
            neg = float(example_artist[neg_col]) if neg_col and pd.notna(example_artist[neg_col]) else 0.0
            neu = float(example_artist[neu_col]) if neu_col and pd.notna(example_artist[neu_col]) else 0.0
            total = pos + neg + neu

            if total > 0:
                vals = [pos, neg, neu]
                labels = ['Positive', 'Negative', 'Neutral']
                colors = ['#8fd3bf', '#ff9a9e', '#e6e6e6']

                plt.figure(figsize=(7, 7))
                plt.pie(vals, labels=labels, autopct='%1.1f%%', startangle=90, colors=colors)
                centre_circle = plt.Circle((0, 0), 0.70, fc='white')
                fig = plt.gcf()
                fig.gca().add_artist(centre_circle)
                plt.title(f'Average YouTube Comment Sentiment for {artist_name}', fontsize=14)
                plt.axis('equal')
                plt.tight_layout()
                safe_savefig(f'{artist_name.replace(" ", "_").lower()}_yt_sentiment.png')
                plt.close()
            else:
                print(f"Skipping sentiment chart for {artist_name}: no sentiment data available.")
        else:
            print("Skipping sentiment donut: no sentiment columns found.")
    else:
        print("Skipping sentiment donut: no artist data available.")

    # ---------- 6) Average Popularity Index by Genre ----------
    if not df_genre.empty and {'primary_genre', 'avg_popularity_index'}.issubset(df_genre.columns):
        plt.figure(figsize=(10, 7))
        sns.barplot(
            x='primary_genre',
            y='avg_popularity_index',
            data=df_genre.sort_values(by='avg_popularity_index', ascending=False),
            palette='cubehelix'
        )
        plt.title('Average Combined Popularity Index by Primary Genre', fontsize=16)
        plt.xlabel('Primary Genre', fontsize=12)
        plt.ylabel('Average Popularity Index', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        safe_savefig('avg_popularity_by_genre.png')
        plt.close()
    else:
        print("Skipping genre chart: genre metrics file missing or columns absent.")

    print("Visualization generation complete.")


if __name__ == "__main__":
    df_artists, df_yt_age, df_genre = load_analyzed_data()
    create_visualizations(df_artists, df_yt_age, df_genre)
