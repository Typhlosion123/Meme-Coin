import requests
import json
import time
import csv
from datetime import datetime, timedelta
import os
import pandas as pd # Import pandas

# --- Configuration ---
COIN_ID = "bitcoin" # CoinGecko ID for Bonk Coin (changed from MOG)
DAYS_HISTORY = 365   # Get 365 days of historical data (max for free tier daily granularity)
DELAY_BETWEEN_REQUESTS_SECONDS = 2 # Delay between API calls to respect rate limits

COINGECKO_API_KEY = os.getenv('COIN_GECKO') # User provided key

if not COINGECKO_API_KEY:
    print("ERROR: CoinGecko API Key not found!")
    print("Please set the COINGECKO_API_KEY environment variable or paste your key directly in the script.")
    print("Exiting...")
    exit() # Exit if API key is critical and not found

# --- API Helper Function ---
def make_coingecko_request(url, params):
    """
    Helper function to make requests to CoinGecko API with the API key.
    """
    if COINGECKO_API_KEY:
        params["x_cg_demo_api_key"] = COINGECKO_API_KEY
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        if e.response.status_code == 401:
            print("This often means your API key is missing, invalid, or required for this endpoint. Please double-check your API key's validity and permissions.")
        elif e.response.status_code == 429:
            print("Rate limit exceeded. Please wait and try again later or increase DELAY_BETWEEN_REQUESTS_SECONDS.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Network/Connection Error: {e}")
        return None

# --- API Functions ---
def get_market_chart_data(coin_id, days):
    """
    Fetches historical market data (prices, market caps, total volumes).
    For days > 90, granularity is daily.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days,
        "interval": "daily" # Explicitly request daily interval for consistency
    }
    return make_coingecko_request(url, params)

def get_ohlc_data(coin_id, days):
    """
    Fetches OHLC data for a specific cryptocurrency.
    For days > 90, granularity is daily.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
    params = {
        "vs_currency": "usd",
        "days": days, 
    }
    return make_coingecko_request(url, params)


def add_price_movement_label(csv_path, output_path):
    # Load the CSV
    df = pd.read_csv(csv_path)

    # Make sure 'price' and 'date' columns exist
    if 'price' not in df.columns or 'date' not in df.columns:
        raise ValueError("CSV must contain 'date' and 'price' columns.")

    # Sort by date to ensure correct order
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values('date', inplace=True)

    # Compare current price to next day's price
    df['next_day_price'] = df['price'].shift(-1)

    def label_change(row):
        if pd.isna(row['next_day_price']):
            return 'N/A'  # No next day
        elif row['next_day_price'] > row['price']:
            return 1
        elif row['next_day_price'] < row['price']:
            return 0
        else:
            return ''

    df['next_day_movement'] = df.apply(label_change, axis=1)

    # Drop helper column and save result
    df.drop(columns=['next_day_price'], inplace=True)
    df.to_csv(output_path, index=False)
    print(f"Updated CSV saved to {output_path}")

# --- Main Execution ---
if __name__ == "__main__":
    print(f"Starting data retrieval for {COIN_ID}...")

    # --- Fetch Market Chart Data (Price, Market Cap, Volume) ---
    print(f"Fetching market chart data for last {DAYS_HISTORY} days...")
    market_chart_data = get_market_chart_data(COIN_ID, DAYS_HISTORY)
    time.sleep(DELAY_BETWEEN_REQUESTS_SECONDS) # Respect API limits

    # --- Fetch OHLC Data (Open, High, Low, Close) ---
    print(f"Fetching OHLC data for last {DAYS_HISTORY} days...")
    ohlc_data = get_ohlc_data(COIN_ID, DAYS_HISTORY)
    time.sleep(DELAY_BETWEEN_REQUESTS_SECONDS) # Respect API limits

    if not market_chart_data or not ohlc_data:
        print("Failed to retrieve necessary historical data. Cannot proceed.")
        exit()

    # --- Process Market Chart Data into DataFrame ---
    print("Processing market chart data into DataFrame...")
    prices = market_chart_data.get('prices', [])
    market_caps = market_chart_data.get('market_caps', [])
    total_volumes = market_chart_data.get('total_volumes', [])

    df_market = pd.DataFrame({
        'timestamp': [p[0] for p in prices],
        'price': [p[1] for p in prices],
        'market_cap': [mc[1] for mc in market_caps],
        'volume': [tv[1] for tv in total_volumes]
    })
    df_market['date'] = pd.to_datetime(df_market['timestamp'], unit='ms').dt.date # Convert to date only for merging
    df_market.set_index('date', inplace=True)
    df_market.drop('timestamp', axis=1, inplace=True) # Drop original timestamp column


    # --- Process OHLC Data into DataFrame ---
    print("Processing OHLC data into DataFrame...")
    df_ohlc = pd.DataFrame(ohlc_data, columns=['timestamp', 'open', 'high', 'low', 'close'])
    df_ohlc['date'] = pd.to_datetime(df_ohlc['timestamp'], unit='ms').dt.date # Convert to date only for merging
    df_ohlc.set_index('date', inplace=True)
    df_ohlc.drop('timestamp', axis=1, inplace=True) # Drop original timestamp column


    # --- Merge DataFrames ---
    # Merge on the 'date' index. An outer merge ensures all dates are kept,
    # filling with NaN where data might be missing from one source.
    print("Merging DataFrames...")
    df_combined = pd.merge(df_market, df_ohlc, left_index=True, right_index=True, how='outer')

    # Reorder columns to have OHLCV together, then price/cap/volume
    # Using a list of desired columns for specific order
    desired_columns = [
        'open', 'high', 'low', 'close', 'volume', # OHLCV
        'price', # Price (often close, but good to have both)
        'market_cap' # Market Cap
    ]
    # Filter to only include columns that actually exist after the merge
    df_combined = df_combined[ [col for col in desired_columns if col in df_combined.columns] ]

    # --- Save to CSV ---
    output_filename = f"coin_data/{COIN_ID}_data.csv"
    df_combined.to_csv(output_filename)
    add_price_movement_label(output_filename, output_filename)
    print(f"\nSuccessfully saved combined historical data to {output_filename}")
    print(f"DataFrame head:\n{df_combined.head()}")
    print(f"DataFrame info:\n{df_combined.info()}")

    print("\nProcess complete.")