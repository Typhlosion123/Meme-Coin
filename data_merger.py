import pandas as pd
import os

def merge_sentiment_data(main_file: str, sentiment_file: str, output_file: str):
    """
    Merges sentiment scores from one CSV into another based on a common 'Date' column.

    Args:
        main_file (str): Path to the main CSV file.
        sentiment_file (str): Path to the CSV file containing sentiment scores.
        output_file (str): Path for the new, merged CSV file.
    """
    # --- Step 1: Read both CSV files ---
    try:
        main_df = pd.read_csv(main_file)
        sentiment_df = pd.read_csv(sentiment_file)
        print(f"\nSuccessfully loaded '{main_file}' and '{sentiment_file}'.")
    except FileNotFoundError as e:
        print(f"Error: Could not find a required file. {e}")
        return
    
    # --- Step 2: Ensure 'Date' columns are in datetime format for accurate matching ---
    main_df['Date'] = pd.to_datetime(main_df['Date'])
    sentiment_df['Date'] = pd.to_datetime(sentiment_df['Date'])

    # --- Step 3: Aggregate sentiment scores by date ---
    # If a date has multiple entries in the sentiment file, sum their scores.
    print("Aggregating sentiment scores by date...")
    sentiment_agg = sentiment_df.groupby('Date')['weighted_sentiment_score'].sum().reset_index()
    
    # Rename the column to avoid conflicts during the merge if it already exists
    sentiment_agg.rename(columns={'weighted_sentiment_score': 'new_sentiment_score'}, inplace=True)

    # --- Step 4: Merge the main data with the aggregated sentiment data ---
    # A 'left' merge keeps all rows from the main_df and adds data where dates match.
    print("Merging dataframes on 'Date' column...")
    merged_df = pd.merge(main_df, sentiment_agg, on='Date', how='left')

    # --- Step 5: Combine new scores with existing scores ---
    # If the main file already has a 'weighted_sentiment_score' column, add the new scores.
    # Otherwise, create the column from the new scores.
    if 'weighted_sentiment_score' in merged_df.columns:
        print("Existing 'weighted_sentiment_score' column found. Adding new scores to it.")
        # Fill NaN values with 0 for both columns before adding them together
        merged_df['weighted_sentiment_score'] = merged_df['weighted_sentiment_score'].fillna(0) + merged_df['new_sentiment_score'].fillna(0)
    else:
        print("No existing 'weighted_sentiment_score' column found. Creating it.")
        merged_df['weighted_sentiment_score'] = merged_df['new_sentiment_score'].fillna(0)
    
    # Drop the temporary 'new_sentiment_score' column
    merged_df.drop(columns=['new_sentiment_score'], inplace=True)

    # --- Step 6: Save the result to a new CSV file ---
    try:
        merged_df.to_csv(output_file, index=False)
        print(f"\n✅ Successfully merged data and saved to '{output_file}'.")
    except Exception as e:
        print(f"An error occurred while saving the file: {e}")


# --- Main execution block ---
if __name__ == "__main__":
    # Define the file paths
    main_csv_path = 'coin_data/doge_data.csv'
    sentiment_csv_path = 'reddit_data/doge_reddit_data.csv'
    output_csv_path = 'training_data/doge.csv'

    # Run the main merging function
    merge_sentiment_data(
        main_file=main_csv_path,
        sentiment_file=sentiment_csv_path,
        output_file=output_csv_path
    )

    # You can uncomment the line below to see the final DataFrame in the console.
    # print("\n--- Content of the final merged file ---")
    # print(pd.read_csv(output_csv_path))
