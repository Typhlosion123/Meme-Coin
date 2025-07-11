import pandas as pd
import os
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def setup_vader():
    """
    Attempts to download the VADER lexicon if it's not already present.
    This is required for the SentimentIntensityAnalyzer to work.
    """
    try:
        # Check if the lexicon is already available
        nltk.data.find('sentiment/vader_lexicon.zip')
    except nltk.downloader.DownloadError:
        # If not found, try to download it
        print("Downloading VADER lexicon for sentiment analysis (one-time setup)...")
        nltk.download('vader_lexicon')

def analyze_sentiment_score(text: str, sid: SentimentIntensityAnalyzer) -> float:
    """
    Analyzes the sentiment of a given text string and returns only the compound score.

    Args:
        text (str): The text to analyze.
        sid (SentimentIntensityAnalyzer): An instance of the VADER sentiment analyzer.

    Returns:
        float: The numerical compound score.
    """
    # Ensure the input is a string, handle potential float/NaN values
    if not isinstance(text, str):
        return 0.0

    scores = sid.polarity_scores(text)
    return scores['compound']

def process_reddit_csv_weighted(file_path: str):
    """
    Reads a CSV, performs sentiment analysis, and calculates a final weighted
    sentiment score based on the post's score.

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
        print("Please run the following in a Python terminal: import nltk; nltk.download('vader_lexicon')")
        return

    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded '{file_path}'. Original shape: {df.shape[0]} rows, {df.shape[1]} columns.")
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return

    # --- Step 2: Perform Sentiment Analysis ---
    columns_to_analyze = ['Post Title', 'Post Description', 'Comment 1', 'Comment 2', 'Comment 3', 'Comment 4', 'Comment 5']
    sentiment_score_columns = []

    for col in columns_to_analyze:
        if col in df.columns:
            sentiment_score_col = f"{col}_score"
            print(f"Analyzing sentiment for column: '{col}'...")
            
            df[sentiment_score_col] = df[col].fillna('').apply(lambda text: analyze_sentiment_score(text, sid))
            sentiment_score_columns.append(sentiment_score_col)
        else:
            print(f"Warning: Column '{col}' not found in the CSV. Skipping.")

    # --- Step 3: Calculate the Final Weighted Score ---
    if 'Score' not in df.columns:
        print("Error: 'Score' column not found. Cannot calculate weighted score.")
        return

    # Ensure the 'Score' column is numeric, replacing non-numeric values with 0
    df['Score'] = pd.to_numeric(df['Score'], errors='coerce').fillna(0)
    
    # Sum all the individual sentiment scores
    df['total_sentiment_score'] = df[sentiment_score_columns].sum(axis=1)

    # --- MODIFICATION: Handle zero scores ---
    # Create a multiplier. Default to the post's score.
    multiplier = df['Score'].copy()
    # Where the score is 0, set the multiplier to 1 to preserve the sentiment score.
    multiplier[df['Score'] == 0] = 1
    
    # Calculate the weighted score using the new multiplier
    df['weighted_sentiment_score'] = df['total_sentiment_score'] * multiplier
    # --- END MODIFICATION ---
    
    print("\nCalculated total and weighted sentiment scores.")

    # --- Step 4: Save the enhanced DataFrame back to the CSV ---
    try:
        df.to_csv(file_path, index=False)
        print(f"\nAnalysis complete. The results have been saved back to '{file_path}'.")
    except Exception as e:
        print(f"An error occurred while saving the updated file: {e}")

# --- Main execution block ---
if __name__ == "__main__":
    # Define the path for the CSV file.
    csv_file_path = "reddit_data/doge_reddit_data.csv"

    # This script assumes the file already exists. If not, you would create it
    # with your data scraping process.
    if not os.path.exists(csv_file_path):
        print(f"Error: File not found at '{csv_file_path}'. Please ensure the file exists before running analysis.")
    else:
        print("\n--- Starting Weighted Reddit Sentiment Analysis Process ---")
        process_reddit_csv_weighted(csv_file_path)
    
    # You can uncomment the line below to see the final DataFrame in the console.
    # print("\n--- Content of the file after analysis ---")
    # print(pd.read_csv(csv_file_path))