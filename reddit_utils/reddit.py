import praw
import datetime
import csv
import os
import pandas as pd # Import pandas

# Setup Reddit client
# Replace with your actual Reddit API credentials
reddit = praw.Reddit(
    client_id="ZhGE7pDT31-4xWBSL1CZHA",
    client_secret="jq31Lq0Fi_HJADbvN-hMvOnMO8atlw",
    user_agent="meme-coin-research by /u/Normal-Organization8"
)

# Define crypto-related subreddits
crypto_subreddits = [
    "CryptoCurrency", "CryptoMarkets", "CryptoMoonShots", "Altcoin", "MemeCoins"
]

# CSV setup for Reddit posts output
output_filename = f"/reddit_data/doge_reddit_data.csv"
os.makedirs("output", exist_ok=True)

# List to store data for pandas DataFrame
all_posts_data = []

print(f"🚀 Starting Reddit post scraping for all relevant posts, including descriptions.")
print(f"Targeting subreddits: {', '.join(crypto_subreddits)}")

for sub in crypto_subreddits:
    print(f"\n🔍 Searching r/{sub} for 'DOGE' posts...")
    subreddit = reddit.subreddit(sub)

    # Search for "DOGE" posts from the last year
    # Set limit=None to search all available posts within the time filter
    for post in subreddit.search("DOGE", sort="relevance", time_filter="year", limit=None):
        post_date = datetime.datetime.fromtimestamp(post.created_utc, tz=datetime.timezone.utc).date()

        print(f"✅ Processing post: {post.title} (Date: {post_date})")

        # Fetch top 5 comments
        post.comments.replace_more(limit=0)
        top_comments = []
        for i, comment in enumerate(post.comments):
            if i >= 5:
                break
            if hasattr(comment, 'body'):
                # Replace newlines in comment body for better CSV formatting
                top_comments.append(comment.body.strip().replace("\n", " "))
            else:
                top_comments.append("")

        # Pad comments list to ensure exactly 5 elements
        while len(top_comments) < 5:
            top_comments.append("")

        # Get the post description (selftext)
        # If the post is a link post, selftext might be empty.
        # Ensure newlines are replaced for better CSV formatting.
        post_description = post.selftext.strip().replace("\n", " ") if hasattr(post, 'selftext') else ""


        # Append post data to our list
        all_posts_data.append({
            "Subreddit": sub,
            "Post Title": post.title,
            "Post URL": post.url,
            "Date": post_date,
            "Score": post.score,
            "Post Description": post_description, # Added Post Description
            "Comment 1": top_comments[0] if len(top_comments) > 0 else "",
            "Comment 2": top_comments[1] if len(top_comments) > 1 else "",
            "Comment 3": top_comments[2] if len(top_comments) > 2 else "",
            "Comment 4": top_comments[3] if len(top_comments) > 3 else "",
            "Comment 5": top_comments[4] if len(top_comments) > 4 else ""
        })

# Create a pandas DataFrame from the collected data
df = pd.DataFrame(all_posts_data)

# Save the DataFrame to a CSV file
try:
    df.to_csv(output_filename, index=False, encoding="utf-8")
    print(f"\n✅ Done! All relevant Reddit posts, top 5 comments, and descriptions saved to: {output_filename}")
except Exception as e:
    print(f"❌ An error occurred while saving the DataFrame to CSV: {e}")
