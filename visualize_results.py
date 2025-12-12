import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.config import PROCESSED_DATA_DIR, RESULTS_DIR

INPUT_FILE = os.path.join(PROCESSED_DATA_DIR, 'final_ranked_artists.csv')

def create_charts():
    if not os.path.exists(INPUT_FILE):
        print("Processed data missing.")
        return

    df = pd.read_csv(INPUT_FILE)
    sns.set_theme(style="whitegrid")
    
    # Chart 1: Ranking Bar
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df, x='popularity_score_100', y='artist', palette='viridis')
    plt.title('Combined Popularity Index', fontsize=16)
    plt.xlabel('Score (0-100)')
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, '1_ranking_bar.png'))
    plt.close()
    
    # Chart 2: Dominance Scatter
    plt.figure(figsize=(10, 8))
    sns.scatterplot(data=df, x='norm_youtube', y='norm_tiktok', s=200, hue='artist', legend=False)
    for i in range(df.shape[0]):
        plt.text(df.norm_youtube[i]+0.02, df.norm_tiktok[i], df.artist[i], fontsize=11, weight='bold')
    
    plt.title('Platform Dominance: YouTube vs. TikTok', fontsize=16)
    plt.xlabel('YouTube (Normalized)')
    plt.ylabel('TikTok (Normalized)')
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, '2_dominance_scatter.png'))
    plt.close()
    
    print("Charts generated in results/ folder.")

if __name__ == "__main__":
    create_charts()
