import os
import sys
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.config import PROCESSED_DATA_DIR, RESULTS_DIR

INPUT_FILE = os.path.join(PROCESSED_DATA_DIR, 'final_ranked_artists.csv')

def generate_report():
    if not os.path.exists(INPUT_FILE):
        print("Processed data missing.")
        return

    df = pd.read_csv(INPUT_FILE)
    report_path = os.path.join(RESULTS_DIR, 'analysis_summary.txt')
    
    with open(report_path, 'w') as f:
        f.write("=== FINAL PROJECT ANALYSIS REPORT ===\n\n")
        
        # Winner
        top = df.iloc[0]
        f.write(f"WINNER: {top['artist']}\n")
        f.write(f"Score: {top['popularity_score_100']}/100\n")
        f.write(f"Stats: {top['youtube_total_views']:,} YT Views | {top['tiktok_post_count']:,} TikTok Posts\n\n")
        
        # Insights
        yt_dom = df.sort_values('norm_youtube', ascending=False).iloc[0]['artist']
        tk_dom = df.sort_values('norm_tiktok', ascending=False).iloc[0]['artist']
        f.write(f"YouTube Dominance Leader: {yt_dom}\n")
        f.write(f"TikTok Viral Leader: {tk_dom}\n\n")
        
        # Table
        f.write("--- FULL RANKING ---\n")
        f.write(df[['rank', 'artist', 'popularity_score_100', 'youtube_total_views', 'tiktok_post_count']].to_string(index=False))
    
    print(f"Report generated: {report_path}")

if __name__ == "__main__":
    generate_report()
