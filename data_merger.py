import pandas as pd
import os

def merge_all_data(main_file: str, reddit_file: str, twitter_file: str, output_file: str):
    """
    Merges sentiment scores from Reddit and reformatted Twitter CSVs into a main data CSV.

    Args:
        main_file (str): Path to the main data CSV.
        reddit_file (str): Path to the Reddit sentiment CSV.
        twitter_file (str): Path to the Twitter sentiment CSV.
        output_file (str): Path for the new, merged CSV file.
    """
    # --- Step 1: Read all three CSV files ---
    try:
        main_df = pd.read_csv(main_file)
        reddit_df = pd.read_csv(reddit_file)
        twitter_df = pd.read_csv(twitter_file)
        print(f"\nSuccessfully loaded all source files.")
    except FileNotFoundError as e:
        print(f"Error: Could not find a required file. {e}")
        return

    # --- Step 2: Ensure all 'Date' columns are in a consistent datetime format ---
    # Pandas can typically infer the 'M/D/YYYY' format automatically.
    main_df['Date'] = pd.to_datetime(main_df['Date'])
    reddit_df['Date'] = pd.to_datetime(reddit_df['Date'])
    twitter_df['Date'] = pd.to_datetime(twitter_df['Date'])

    # --- Step 3: Aggregate scores from both sentiment files ---
    print("Aggregating sentiment scores by date...") 
    reddit_agg = reddit_df.groupby('Date')['weighted_sentiment_score'].sum().reset_index()
    twitter_agg = twitter_df.groupby('Date')['final_weighted_score'].sum().reset_index()
    
    # Rename columns to avoid conflicts and for clarity
    reddit_agg.rename(columns={'weighted_sentiment_score': 'reddit_sentiment_score'}, inplace=True)
    twitter_agg.rename(columns={'final_weighted_score': 'twitter_sentiment_score'}, inplace=True)

    # --- Step 4: Merge all dataframes together ---
    print("Merging dataframes...")
    # First, merge main with Reddit data
    merged_df = pd.merge(main_df, reddit_agg, on='Date', how='left')
    # Then, merge the result with Twitter data
    merged_df = pd.merge(merged_df, twitter_agg, on='Date', how='left')

    # --- Step 5: Finalize sentiment score columns ---
    print("Finalizing sentiment score columns...")
    # Fill any dates that didn't have sentiment data with 0
    merged_df['reddit_sentiment_score'] = merged_df['reddit_sentiment_score'].fillna(0)
    merged_df['twitter_sentiment_score'] = merged_df['twitter_sentiment_score'].fillna(0)
    
    # --- Step 6: Save the result ---
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        merged_df.to_csv(output_file, index=False)
        print(f"\nSuccessfully merged data and saved to '{output_file}'.")
    except Exception as e:
        print(f"An error occurred while saving the file: {e}")


# --- Main execution block ---
if __name__ == "__main__":
    # Define the file paths
    COIN_NAME = "bitcoin"
    main_csv_path = f'coin_data/{COIN_NAME}_data.csv'
    reddit_csv_path = f'reddit_data/{COIN_NAME}_reddit_data.csv'
    twitter_reformatted_path = f'twitter_data/{COIN_NAME}_twitter_data.csv'
    output_csv_path = f'training_data/{COIN_NAME}_final.csv'

    # Run the main merging function
    merge_all_data(
        main_file=main_csv_path,
        reddit_file=reddit_csv_path,
        twitter_file=twitter_reformatted_path,
        output_file=output_csv_path
    )
