import os
import sys
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

def load_latest_raw():
    files = [f for f in os.listdir(RAW_DATA_DIR) if f.startswith('artists_summary')]
    if not files: return None
    latest = sorted(files)[-1]
    print(f"Loading: {latest}")
    return pd.read_csv(os.path.join(RAW_DATA_DIR, latest))

def process_data(df):
    scaler = MinMaxScaler()
    
    # Normalize
    df['norm_youtube'] = scaler.fit_transform(df[['youtube_total_views']])
    df['norm_tiktok'] = scaler.fit_transform(df[['tiktok_post_count']])
    
    # Calculate Index: 55% YT / 45% TikTok
    df['popularity_index'] = (0.55 * df['norm_youtube']) + (0.45 * df['norm_tiktok'])
    df['popularity_score_100'] = (df['popularity_index'] * 100).round(2)
    
    # Rank
    df_ranked = df.sort_values('popularity_index', ascending=False).reset_index(drop=True)
    df_ranked['rank'] = df_ranked.index + 1
    return df_ranked

if __name__ == "__main__":
    df = load_latest_raw()
    if df is not None:
        clean_df = process_data(df)
        outfile = os.path.join(PROCESSED_DATA_DIR, 'final_ranked_artists.csv')
        clean_df.to_csv(outfile, index=False)
        print(f"Saved processed data to: {outfile}")
        print(clean_df[['rank', 'artist', 'popularity_score_100']])
    else:
        print("No raw data found!")
