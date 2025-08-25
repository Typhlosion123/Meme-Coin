import pandas as pd
import os

def format_twitter_dates(input_file: str, output_file: str):
    """
    Reads a Twitter CSV, converts the date format, and saves a new CSV.

    Args:
        input_file (str): Path to the source Twitter CSV.
        output_file (str): Path for the new, reformatted CSV.
    """
    # --- Step 1: Read the source CSV file ---
    try:
        twitter_df = pd.read_csv(input_file)
        print(f"\nSuccessfully loaded '{input_file}'.")
    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
        return

    # --- Step 2: Convert the 'tweet_created_at' column to datetime objects ---
    # This step parses the complex date string into a format pandas understands.
    # The .dt.normalize() part sets the time to midnight, keeping only the date.
    print("Parsing original date format...")
    parsed_dates = pd.to_datetime(
        twitter_df['tweet_created_at'], format='%a %b %d %H:%M:%S %z %Y'
    ).dt.normalize()

    # --- Step 3: Create the new 'Date' column in 'M/D/YYYY' format ---
    # We build the string manually to ensure there are no leading zeros (e.g., '9/17/2024' instead of '09/17/2024').
    print("Creating new 'Date' column in M/D/YYYY format...")
    twitter_df['Date'] = (
        parsed_dates.dt.month.astype(str) + '/' +
        parsed_dates.dt.day.astype(str) + '/' +
        parsed_dates.dt.year.astype(str)
    )

    # --- Step 4: Reorder columns and save the result ---
    # Move the new 'Date' column to the front and drop the old one.
    final_df = twitter_df[['Date'] + [col for col in twitter_df.columns if col != 'Date']]
    final_df = final_df.drop(columns=['tweet_created_at'])

    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        final_df.to_csv(output_file, index=False)
        print(f"\nSuccessfully reformatted dates and saved to '{output_file}'.")
    except Exception as e:
        print(f"An error occurred while saving the file: {e}")


# --- Main execution block ---
if __name__ == "__main__":
    # Define the file paths
    twitter_input_path = 'twitter_data/bitcoin_twitter_data.csv'
    reformatted_output_path = 'twitter_data/bitcoin_twitter_data.csv'

    # Run the main formatting function
    format_twitter_dates(
        input_file=twitter_input_path,
        output_file=reformatted_output_path
    )
