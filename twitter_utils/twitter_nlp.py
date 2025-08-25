import pandas as pd
import os
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import numpy as np

def setup_vader():
    """
    Attempts to download the VADER lexicon if it's not already present.
    """
    try:
        nltk.data.find('sentiment/vader_lexicon.zip')
    except nltk.downloader.DownloadError:
        print("Downloading VADER lexicon for sentiment analysis (one-time setup)...")
        nltk.download('vader_lexicon')

def analyze_sentiment_score(text: str, sid: SentimentIntensityAnalyzer) -> float:
    """
    Analyzes the sentiment of a given text string and returns the compound score.
    """
    if not isinstance(text, str):
        return 0.0
    scores = sid.polarity_scores(text)
    return scores['compound']

def process_advanced_tweet_analysis(file_path: str):
    """
    Reads a CSV, performs sentiment analysis, and adds weighted scores
    based on favorites and follower influence.

    Args:
        file_path (str): The full path to the CSV file.
    """
    # --- Step 1: Setup and Validation ---
    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' was not found.")
        return
        
    setup_vader()
    
    try:
        sid = SentimentIntensityAnalyzer()
    except LookupError:
        print("\n--- NLTK VADER Lexicon Error ---")
        print("The VADER lexicon could not be found or downloaded.")
        return

    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded '{file_path}'. Original shape: {df.shape[0]} rows, {df.shape[1]} columns.")
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return

    # --- Step 2: Initial Sentiment Analysis ---
    if 'tweet_text' not in df.columns:
        print("Error: Column 'tweet_text' not found. Aborting.")
        return
        
    print("Analyzing sentiment for 'tweet_text'...")
    df['sentiment_score'] = df['tweet_text'].fillna('').apply(lambda text: analyze_sentiment_score(text, sid))

    # --- Step 3: Calculate Weighted Favorite Score ---
    if 'tweet_favorite_count' not in df.columns:
        print("Warning: 'tweet_favorite_count' column not found. Skipping weighted favorite score.")
        df['weighted_favorite_score'] = df['sentiment_score'] # Default to base sentiment if no favs
    else:
        print("Calculating weighted favorite score...")
        # Ensure the column is numeric, filling non-numeric values with 0
        df['tweet_favorite_count'] = pd.to_numeric(df['tweet_favorite_count'], errors='coerce').fillna(0)
        df['weighted_favorite_score'] = df['sentiment_score'] * df['tweet_favorite_count']

    # --- Step 4: Calculate Follower Influence Score (Logarithmic Weighting) ---
    # Using the corrected column name 'user_followers_count'
    if 'user_followers_count' not in df.columns:
        print("Warning: 'user_followers_count' column not found. Skipping follower analysis.")
        df['follower_influence_score'] = 1 # Default to 1 if no follower count
    else:
        print("Analyzing user influence using logarithmic weighting...")
        # Ensure the column is numeric
        df['user_followers_count'] = pd.to_numeric(df['user_followers_count'], errors='coerce').fillna(0)

        # Use np.log1p which calculates log(1 + x) to gracefully handle users with 0 followers.
        # This gives a better representation of influence than raw counts.
        df['follower_influence_score'] = np.log1p(df['user_followers_count'])

    # --- Step 5: Calculate Final Combined Score ---
    print("Calculating final weighted score...")
    df['final_weighted_score'] = df['weighted_favorite_score'] * df['follower_influence_score']


    # --- Step 6: Save the enhanced DataFrame back to the CSV ---
    try:
        df.to_csv(file_path, index=False)
        print(f"\nAnalysis complete. The updated data has been saved back to '{file_path}'.")
    except Exception as e:
        print(f"An error occurred while saving the updated file: {e}")

# --- Main execution block ---
if __name__ == "__main__":
    # Define the path for the CSV file.
    csv_file_path = f"twitter_data/bitcoin_twitter_data.csv"

    print("\n--- Starting Advanced Tweet Analysis Process ---")
    process_advanced_tweet_analysis(csv_file_path)
    
    # You can uncomment the line below to see the final DataFrame in the console.
    # print("\n--- Content of the file after analysis ---")
    # print(pd.read_csv(csv_file_path))