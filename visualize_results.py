# src/visualize_results.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- Configuration ---
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data/processed')
RESULTS_DIR = os.path.join(os.path.dirname(__file__), '../results')
os.makedirs(RESULTS_DIR, exist_ok=True) # Ensure results directory exists

def load_analyzed_data():
    """Loads the analyzed CSV data from data/processed."""
    analyzed_artists_filepath = os.path.join(PROCESSED_DATA_DIR, 'analyzed_artists_data.csv')
    analyzed_yt_age_performance_filepath = os.path.join(PROCESSED_DATA_DIR, 'analyzed_yt_age_performance.csv')
    analyzed_sp_age_performance_filepath = os.path.join(PROCESSED_DATA_DIR, 'analyzed_sp_age_performance.csv')
    analyzed_genre_metrics_filepath = os.path.join(PROCESSED_DATA_DIR, 'analyzed_genre_metrics.csv')

    if not all(os.path.exists(f) for f in [analyzed_artists_filepath, analyzed_yt_age_performance_filepath,
                                           analyzed_sp_age_performance_filepath, analyzed_genre_metrics_filepath]):
        print("Analyzed data files not found. Please run run_analysis.py first.")
        return None, None, None, None

    df_analyzed_artists = pd.read_csv(analyzed_artists_filepath)
    df_yt_age_performance = pd.read_csv(analyzed_yt_age_performance_filepath)
    df_sp_age_performance = pd.read_csv(analyzed_sp_age_performance_filepath)
    df_genre_metrics = pd.read_csv(analyzed_genre_metrics_filepath)

    return df_analyzed_artists, df_yt_age_performance, df_sp_age_performance, df_genre_metrics

def create_visualizations(df_analyzed_artists, df_yt_age_performance, df_sp_age_performance, df_genre_metrics):
    """Generates and saves all project visualizations."""
    print("Starting visualization generation...")

    # Set style for plots
    sns.set_theme(style="whitegrid")
    
    # --- 1. Comparative Bar Chart: Combined Popularity Index ---
    plt.figure(figsize=(12, 7))
    sns.barplot(x='artist_name', y='combined_popularity_index', data=df_analyzed_artists.sort_values(by='combined_popularity_index', ascending=False), palette='viridis')
    plt.title('Combined Popularity Index by Artist', fontsize=16)
    plt.xlabel('Artist', fontsize=12)
    plt.ylabel('Normalized Combined Popularity Index', fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=10)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'combined_popularity_index.png'))
    plt.close()
    print("Generated: combined_popularity_index.png")

    # --- 2. Content Age vs. Performance: YouTube Videos (Average Views) ---
    # Define a consistent order for age categories
    age_category_order = ['0-1 Month', '1-3 Months', '3-12 Months', '1-3 Years', '3+ Years']
    df_yt_age_performance['age_category'] = pd.Categorical(df_yt_age_performance['age_category'], categories=age_category_order, ordered=True)

    plt.figure(figsize=(14, 8))
    sns.barplot(x='age_category', y='avg_views', hue='artist_id', data=df_yt_age_performance.sort_values(by=['artist_id', 'age_category']), palette='deep')
    plt.title('Average YouTube Video Views by Content Age and Artist', fontsize=16)
    plt.xlabel('Content Age Category', fontsize=12)
    plt.ylabel('Average Views', fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=10)
    plt.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'yt_avg_views_by_age.png'))
    plt.close()
    print("Generated: yt_avg_views_by_age.png")

    # --- 3. Content Age vs. Performance: Spotify Tracks (Average Popularity) ---
    df_sp_age_performance['age_category'] = pd.Categorical(df_sp_age_performance['age_category'], categories=age_category_order, ordered=True)
    
    plt.figure(figsize=(14, 8))
    sns.barplot(x='age_category', y='avg_track_popularity', hue='artist_id', data=df_sp_age_performance.sort_values(by=['artist_id', 'age_category']), palette='muted')
    plt.title('Average Spotify Track Popularity by Content Age and Artist', fontsize=16)
    plt.xlabel('Content Age Category', fontsize=12)
    plt.ylabel('Average Track Popularity (0-100)', fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=10)
    plt.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'sp_avg_popularity_by_age.png'))
    plt.close()
    print("Generated: sp_avg_popularity_by_age.png")


    # --- 4. Scatter Plot: Release Frequency vs. Combined Popularity Index ---
    # Using total content count as a proxy for release frequency (from run_analysis.py)
    plt.figure(figsize=(10, 7))
    sns.scatterplot(x='total_content_count', y='combined_popularity_index', hue='artist_name', data=df_analyzed_artists, s=200, alpha=0.7, palette='tab10')
    plt.title('Content Release Frequency vs. Combined Popularity Index', fontsize=16)
    plt.xlabel('Total Number of Fetched Content Items (Proxy for Release Frequency)', fontsize=12)
    plt.ylabel('Normalized Combined Popularity Index', fontsize=12)
    plt.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'release_frequency_vs_popularity.png'))
    plt.close()
    print("Generated: release_frequency_vs_popularity.png")


    # --- 5. Scatter Plot: Normalized Spotify Popularity vs. Normalized YouTube Subscribers ---
    plt.figure(figsize=(10, 7))
    sns.scatterplot(x='norm_spotify_popularity', y='norm_yt_subscribers', hue='artist_name', data=df_analyzed_artists, s=200, alpha=0.7, palette='tab10')
    plt.title('Normalized Spotify Popularity vs. Normalized YouTube Subscribers', fontsize=16)
    plt.xlabel('Normalized Spotify Artist Popularity', fontsize=12)
    plt.ylabel('Normalized YouTube Channel Subscribers', fontsize=12)
    plt.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'spotify_vs_youtube_popularity.png'))
    plt.close()
    print("Generated: spotify_vs_youtube_popularity.png")

    # --- 6. Pie/Donut Chart: Average YouTube Comment Sentiment per Artist (Example for one artist) ---
    if not df_analyzed_artists.empty and 'sentiment_positive_ratio' in df_analyzed_artists.columns:
        # Get the top artist by popularity for this example
        example_artist_data = df_analyzed_artists.sort_values(by='combined_popularity_index', ascending=False).iloc[0]
        artist_name = example_artist_data['artist_name']
        sentiment_scores = [
            example_artist_data['sentiment_positive_ratio'],
            example_artist_data['sentiment_negative_ratio'],
            example_artist_data['sentiment_neutral_ratio']
        ]
        sentiment_labels = ['Positive', 'Negative', 'Neutral']
        sentiment_colors = ['#8fd3bf', '#ff9a9e', '#e6e6e6'] # Light green, light red, light grey

        if sum(sentiment_scores) > 0: # Ensure there's sentiment data to plot
            plt.figure(figsize=(8, 8))
            plt.pie(sentiment_scores, labels=sentiment_labels, autopct='%1.1f%%', startangle=90, colors=sentiment_colors, pctdistance=0.85)
            # Create a donut chart by adding a white circle in the middle
            centre_circle = plt.Circle((0,0), 0.70, fc='white')
            fig = plt.gcf()
            fig.gca().add_artist(centre_circle)
            plt.title(f'Average YouTube Comment Sentiment for {artist_name}', fontsize=16)
            plt.axis('equal') # Equal aspect ratio ensures that pie is drawn as a circle.
            plt.tight_layout()
            plt.savefig(os.path.join(RESULTS_DIR, f'{artist_name.replace(" ", "_").lower()}_yt_sentiment.png'))
            plt.close()
            print(f"Generated: {artist_name.replace(' ', '_').lower()}_yt_sentiment.png")
        else:
            print(f"Skipping sentiment chart for {artist_name}: No sentiment data available.")

    # --- 7. Categorical Plot: Average Popularity Index by Primary Genre ---
    if not df_genre_metrics.empty and 'primary_genre' in df_genre_metrics.columns:
        plt.figure(figsize=(10, 7))
        sns.barplot(x='primary_genre', y='avg_popularity_index', data=df_genre_metrics.sort_values(by='avg_popularity_index', ascending=False), palette='cubehelix')
        plt.title('Average Combined Popularity Index by Primary Genre', fontsize=16)
        plt.xlabel('Primary Genre', fontsize=12)
        plt.ylabel('Average Normalized Popularity Index', fontsize=12)
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.yticks(fontsize=10)
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, 'avg_popularity_by_genre.png'))
        plt.close()
        print("Generated: avg_popularity_by_genre.png")

    print("All visualizations generated.")


if __name__ == "__main__":
    df_analyzed_artists, df_yt_age_performance, df_sp_age_performance, df_genre_metrics = load_analyzed_data()

    if df_analyzed_artists is not None: # Check if loading was successful
        create_visualizations(df_analyzed_artists, df_yt_age_performance, df_sp_age_performance, df_genre_metrics)
    else:
        print("Skipping visualization due to no analyzed data.")
