# Popularity Analysis of Diverse Musical Artists

## Project Overview
[As per your project proposal's problem statement and objectives]

## Setup and Installation
1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/your-username/your-repo-name.git](https://github.com/your-username/your-repo-name.git)
    cd your-repo-name
    ```

2.  **Prerequisites (Important):**
    * **Google Chrome:** You must have the Google Chrome browser installed on your machine for the TikTok scraper to work.
    * **Python 3.10+**

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *(Ensure your `requirements.txt` includes: `selenium`, `webdriver-manager`, `pandas`, `google-api-python-client`, `scikit-learn`, `matplotlib`, `seaborn`, `python-dotenv`)*

4.  **NLTK Data Download:**
    Open a Python interpreter and run:
    ```python
    import nltk
    nltk.download('punkt')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('vader_lexicon')
    ```

5.  **API Key Configuration:**
    This project relies on the YouTube Data API v3. You need to obtain API credentials:
    * **YouTube:** Get an API Key from [Google Cloud Console](https://console.cloud.google.com/) (enable YouTube Data API v3).
    
    Store this key as an environment variable or in a `.env` file in the root of your project:
    
    ```
    # .env file example
    YOUTUBE_API_KEY="AIzaSyCzy7w8fwDdk9AOy9gJ5ZHGRUN0As4aKP4"
    ```
    The `python-dotenv` library will automatically load this when the scripts run.

## How to Run the Code

### 1. Data Collection (`src/get_data.py`)
This script fetches data from the YouTube Data API and scrapes TikTok hashtag metrics using Selenium.

**Important Note for TikTok Collection:**
This project uses a **Manual-Assist Selenium Scraper** to bypass TikTok's anti-bot restrictions. When you run the script:
1.  A **Google Chrome window** will launch automatically for each artist.
2.  **Action Required:** You must manually close any "Shop" or "Login" popups that appear in the browser.
3.  Verify that the **Post Count** (e.g., "25.4M posts") is visible on the page.
4.  Return to your terminal/console and **press Enter** when prompted to confirm the page is ready. The script will then read the data and proceed to the next artist.

To collect data:
```bash
python src/get_data.py
