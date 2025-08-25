import pandas as pd
import os

def filter_csv_for_doge(file_path: str, coin_name: str):
    """
    Reads a CSV file, filters it to keep only rows containing the word coin name
    (case-insensitive) in any cell, and saves the result back to the same file.

    Args:
        file_path (str): The full path to the CSV file.
    """
    # --- Step 1: Validate file path and read the CSV ---
    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' was not found.")
        return

    try:
        # Read the entire CSV into a pandas DataFrame
        df = pd.read_csv(file_path)
        print(f"Successfully loaded '{file_path}'. Original shape: {df.shape[0]} rows, {df.shape[1]} columns.")
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return

    if df.empty:
        print("The CSV file is empty. No action taken.")
        return

    # --- Step 2: Create a boolean mask for rows to keep ---
    # The mask will be True for rows that should be kept, and False for rows to be removed.
    # We iterate through each row (axis=1). For each row, we do the following:
    # 1. Convert all cells in the row to string type to avoid errors with numbers/dates.
    # 2. Use .str.contains() to check for 'doge'.
    #    - `case=False` makes the search case-insensitive.
    #    - `na=False` treats empty/NaN cells as not containing 'doge'.
    # 3. `.any()` checks if the condition is True for AT LEAST ONE cell in the row.
    mask = df.apply(lambda row: row.astype(str).str.contains(coin_name, case=False, na=False).any(), axis=1)

    # --- Step 3: Apply the mask to filter the DataFrame ---
    df_filtered = df[mask]
    
    kept_rows = len(df_filtered)
    removed_rows = len(df) - kept_rows
    print(f"Filtering complete. Kept {kept_rows} rows and removed {removed_rows} rows.")


    # --- Step 4: Save the filtered DataFrame back to the same CSV file ---
    try:
        # `index=False` prevents pandas from writing the DataFrame index as a new column
        df_filtered.to_csv(file_path, index=False)
        print(f"Successfully saved the filtered data back to '{file_path}'.")
    except Exception as e:
        print(f"An error occurred while saving the file: {e}")

# --- Main execution block ---
if __name__ == "__main__":
    # Define the path to your CSV file here.
    # We'll use a sample file for this example.
    csv_file_path = f"reddit_data/bitcoin_reddit_data.csv"
    
    print("\n--- Starting Filter Process ---")
    # Run the filtering function on the specified file
    filter_csv_for_doge(csv_file_path, "Bitcoin")
