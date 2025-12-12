# The Viral Gap: Comparative Analysis of Artist Popularity (YouTube vs. TikTok)

## Team Members
* **Name:** Henry Yu
* **USC ID:** 1678028702
* **GitHub:** henryyu528
* **Name:** Rohit Praveen
* * **USC ID:** 
* **GitHub:** rohit-design

## Project Overview
This project investigates the correlation between "Legacy Fame" (YouTube Views) and "Viral Relevance" (TikTok Content Creation). By analyzing 10 major artists, we quantify the difference between passive consumption and active user engagement using a custom "Combined Popularity Index."

## Directory Structure
* `data/`: Stores raw CSVs (from scrapers) and processed CSVs (cleaned/ranked).
* `src/`: Python source code for collection, cleaning, and visualization.
* `results/`: Generated charts (PNG) and analysis reports (TXT).

## Setup & Installation

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **API Configuration:**
    Create a `.env` file in the root directory and add your YouTube API Key:
    ```text
    YOUTUBE_API_KEY="AIzaSy..."
    ```

## How to Run
Run the scripts in this order:

1.  **Collect Data:**
    ```bash
    python src/get_data.py
    ```
    *Note: This opens a Chrome window for TikTok. You must manually verify the "Post Count" and press Enter in the terminal for each artist.*

2.  **Clean & Process:**
    ```bash
    python src/clean_data.py
    ```

3.  **Run Analysis (Text Report):**
    ```bash
    python src/run_analysis.py
    ```

4.  **Generate Visualizations:**
    ```bash
    python src/visualize_results.py
    ```
