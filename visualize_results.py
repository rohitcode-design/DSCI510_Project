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

INPUT_FILE = os.path.join(PROCESSED_DATA_DIR, 'final_ranked_artists.csv')

def load_data():
    """Loads the final ranked dataset."""
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        print("Please run clean_data.py before running visualizations.")
        return pd.DataFrame()
    
    df = pd.read_csv(INPUT_FILE)
    return df

def safe_savefig(fig_name):
    """Helper to save figures to RESULTS_DIR."""
    path = os.path.join(RESULTS_DIR, fig_name)
    plt.tight_layout()
    plt.savefig(path, dpi=300) # High resolution for PDF report
    plt.close()
    print(f"Generated Figure: {fig_name}")

def create_visualizations(df):
    """Generates charts based on the available summary data."""
    if df.empty:
        print("No data to visualize.")
        return

    print("Starting visualization generation...")
    sns.set_theme(style="whitegrid")

    # ---------- 1) Combined Popularity Index Bar Chart ----------
    if 'popularity_score_100' in df.columns:
        plt.figure(figsize=(12, 7))
        sns.barplot(
            data=df,
            x='popularity_score_100',
            y='artist',
            palette='viridis'
        )
        plt.title('Final Popularity Ranking (YouTube + TikTok)', fontsize=16)
        plt.xlabel('Popularity Score (0-100)', fontsize=12)
        plt.ylabel('Artist', fontsize=12)
        plt.axvline(x=50, color='r', linestyle='--', alpha=0.5, label='Average')
        safe_savefig('1_popularity_index_ranking.png')
    
    # ---------- 2) Platform Dominance (Scatter Plot) ----------
    if 'norm_youtube' in df.columns and 'norm_tiktok' in df.columns:
        plt.figure(figsize=(10, 8))
        sns.scatterplot(
            data=df, 
            x='norm_youtube', 
            y='norm_tiktok', 
            s=300, 
            hue='artist', 
            palette='deep', 
            legend=False,
            alpha=0.8
        )
        
        # Add labels
        for i in range(df.shape[0]):
            plt.text(
                df.norm_youtube[i]+0.02, 
                df.norm_tiktok[i], 
                df.artist[i], 
                fontsize=11, 
                weight='bold'
            )

        plt.title('Platform Dominance Analysis', fontsize=16)
        plt.xlabel('YouTube Dominance (Normalized Views)', fontsize=12)
        plt.ylabel('TikTok Dominance (Normalized Post Count)', fontsize=12)
        plt.xlim(-0.1, 1.2)
        plt.ylim(-0.1, 1.2)
        
        # Draw quadrants
        plt.axhline(0.5, color='grey', linestyle='--', alpha=0.3)
        plt.axvline(0.5, color='grey', linestyle='--', alpha=0.3)
        plt.text(0.9, 0.9, 'Multi-Platform\nSuperstars', color='green', ha='center')
        plt.text(0.9, 0.1, 'YouTube\nHeavy', color='blue', ha='center')
        plt.text(0.1, 0.9, 'TikTok\nViral', color='purple', ha='center')
        
        safe_savefig('2_platform_dominance_scatter.png')

    # ---------- 3) Raw Scale Comparison (Log Scale) ----------
    # This helps visualize the difference in scale between Views (Billions) and Posts (Millions)
    if 'youtube_total_views' in df.columns and 'tiktok_post_count' in df.columns:
        fig, ax1 = plt.subplots(figsize=(12, 6))

        # Sort by YouTube views for visual clarity
        df_sorted = df.sort_values('youtube_total_views', ascending=False)
        x = np.arange(len(df_sorted))
        width = 0.35

        # Bar 1: YouTube Views
        ax1.bar(x - width/2, df_sorted['youtube_total_views'], width, label='YouTube Views', color='#ff0000', alpha=0.7)
        ax1.set_ylabel('YouTube Views (Billions)', color='#ff0000', fontsize=12)
        ax1.tick_params(axis='y', labelcolor='#ff0000')
        ax1.set_xticks(x)
        ax1.set_xticklabels(df_sorted['artist'], rotation=45, ha='right')

        # Bar 2: TikTok Posts (Twin Axis)
        ax2 = ax1.twinx()
        ax2.bar(x + width/2, df_sorted['tiktok_post_count'], width, label='TikTok Posts', color='#00f2ea', alpha=0.7)
        ax2.set_ylabel('TikTok Post Count (Millions)', color='#008b8b', fontsize=12)
        ax2.tick_params(axis='y', labelcolor='#008b8b')
        
        plt.title('Scale Comparison: Passive Views vs. Active Creations', fontsize=16)
        safe_savefig('3_raw_metrics_comparison.png')

    print("Visualization generation complete.")

if __name__ == "__main__":
    df = load_data()
    create_visualizations(df)
