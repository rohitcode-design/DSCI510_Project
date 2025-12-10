# Popularity Analysis of Diverse Musical Artists

## Project Overview
[As per your project proposal's problem statement and objectives]

## Setup and Installation
1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/your-username/your-repo-name.git
    cd your-repo-name
    ```
2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **NLTK Data Download (for Sentiment Analysis):**
    Open a Python interpreter and run:
    ```python
    import nltk
    nltk.download('punkt')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('vader_lexicon')
    ```
4.  **API Key Configuration:**
    This project relies on the YouTube Data API v3. You need to obtain API credentials for both:
    *   **YouTube:** Get an API Key from [Google Cloud Console](https://console.cloud.google.com/) (enable YouTube Data API v3).
    
    Store these keys as environment variables or in a `.env` file in the root of your project:
    
    ```
    # .env file example
    YOUTUBE_API_KEY="your_youtube_api_key"
    ```
    The `python-dotenv` library (included in `requirements.txt`) will automatically load these when the scripts run.

## How to Run the Code

### 1. Data Collection (`src/get_data.py`)
This script connects to the Spotify and YouTube APIs to fetch raw data for the specified artists.

To collect data:
```bash
python src/get_data.py
