import asyncio
from twikit import Client
from twikit.errors import NotFound  # Import the specific error
import pandas as pd

async def scrape_memecoin():
    """
    Scrapes a specified number of tweets for a search query and saves them to a CSV.
    This version correctly handles pagination, avoids rate limiting with a delay,
    and extracts the URL of the first photo attached to a tweet.
    """
    client = Client("en-US")

    # It's generally better to load cookies if they exist to avoid repeated logins
    try:
        client.load_cookies('cookies.json')
        print("Successfully loaded cookies from file.")
    except FileNotFoundError:
        # Replace with your actual credentials
        USERNAME = 'katieelisluv'
        EMAIL = 'typhlosion197@gmail.com'
        PASSWORD = 'P$1930verde'
        
        print("Cookies not found. Attempting to log in...")
        await client.login(
            auth_info_1=USERNAME,
            auth_info_2=EMAIL,
            password=PASSWORD,
            enable_ui_metrics=True,
        )
        client.save_cookies('cookies.json')
        print("Login successful, cookies saved for future sessions.")

    # --- Pagination Logic ---
    
    # Define the total number of tweets you want to scrape
    max_tweets_to_scrape = 800  # <-- Change this number to your desired amount
    delay_between_requests = 2  # <-- Delay in seconds between fetching pages
    all_tweets_data = []
    search_query = "#dogecoin"

    print(f"Starting to scrape up to {max_tweets_to_scrape} tweets for '{search_query}'...")

    # --- MODIFICATION: Handle error on initial search ---
    try:
        # Get the first batch/page of tweets.
        # Changed product to "Latest" as it can be more reliable than "Top".
        tweets_batch = await client.search_tweet(
            search_query, product="Latest", count=20
        )
    except NotFound:
        print(f"Error: Initial search for '{search_query}' failed with a 404 Not Found error.")
        print("This may be a temporary Twitter API issue. Please try again later.")
        return # Exit the function gracefully
    except Exception as e:
        print(f"An unexpected error occurred during the initial search: {e}")
        return
    # --- END MODIFICATION ---

    # Loop as long as we have a batch of tweets and haven't reached our goal
    while tweets_batch and len(all_tweets_data) < max_tweets_to_scrape:
        # Use a standard 'for' loop to iterate through tweets in the current batch
        for tweet in tweets_batch:
            # Extract photo URL
            photo_url = ''  # Default to an empty string
            if tweet.media:  # Check if the tweet has any media
                for media_item in tweet.media:
                    if media_item.type == 'photo':
                        photo_url = media_item.url  # Get the URL of the first photo
                        break  # Stop after finding the first photo
            
            all_tweets_data.append([
                tweet.user.name,
                tweet.id,
                tweet.full_text,
                tweet.created_at,
                tweet.reply_count,
                tweet.view_count,
                tweet.favorite_count,
                tweet.hashtags,
                photo_url  # Add the photo URL to our data list
            ])
            # Check if we've reached our goal after adding each tweet
            if len(all_tweets_data) >= max_tweets_to_scrape:
                break
        
        # If the inner loop was broken, break the outer loop as well
        if len(all_tweets_data) >= max_tweets_to_scrape:
            break

        print(f"Collected {len(all_tweets_data)} tweets, fetching next page...")
        
        # Add delay to avoid rate limiting
        print(f"Waiting for {delay_between_requests} seconds before next request...")
        await asyncio.sleep(delay_between_requests)

        try:
            # Fetch the next batch of results. This is the correct way to paginate.
            tweets_batch = await tweets_batch.next()
        except NotFound:
            # This is the expected behavior when there are no more tweets to load.
            print("No more pages to fetch. All available tweets have been collected.")
            break
        except Exception as e:
            # Catch any other unexpected errors during pagination.
            print(f"An unexpected error occurred while fetching the next page. Stopping. Error: {e}")
            break

    print(f"\nScraping complete. Scraped a total of {len(all_tweets_data)} tweets.")

    # Create the DataFrame and save to CSV, adding the new 'media' column
    df = pd.DataFrame(
        all_tweets_data, 
        columns=["user", "ID", "text", "time", "replies", "views", "favorite count", "hashtags", "media"]
    )
    df.to_csv("memecoin_tweets.csv", index=False)
    print("✅ Saved to memecoin_tweets.csv")

# Run the async function
if __name__ == "__main__":
    asyncio.run(scrape_memecoin())