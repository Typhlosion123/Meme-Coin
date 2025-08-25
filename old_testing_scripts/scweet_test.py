from Scweet.scweet import Scweet
from Scweet.utils import create_mailtm_email
import asyncio
from time import sleep
import os


proxy = None # add proxy settings. IF the proxy is public, you can provide empty username and password
cookies = None # current library implementation depends on Nodriver cookies handling.
cookies_directory = 'cookies' # directory where you want to save/load the cookies 'username_cookies.dat'
user_agent = None
disable_images = True # disable loading images while fetching
env_path = '.env' # .env path where twitter account credentials are
n_splits = -1 # set the number of splits that you want to perform on the date interval (the bigger the interval and the splits, the bigger the scraped tweets)
concurrency = 5 # tweets and profiles fetching run in parallel (on multiple browser tabs at the same time). Adjust depending on ressources.
headless = False
scroll_ratio = 100 # scrolling ratio while fetching tweets. adjust between 30 to 200 to optimize tweets fetching.
login = True # this is used for get_user_information method. X asks for login sometimes to display user profile.
# You are always required to login for tweets fetching.
# We recommend signing up for twiiter using MailTM or other email providers and setup the code_callback method which will be used internally to get the code from email if requested.
# Scweet already have an internal method that handles the case of MailTM emails.
# Use the function create_mailtm_email it you want to create an email and use it for twitter signup.

scweet = Scweet(proxy, cookies, cookies_directory, user_agent, disable_images, env_path,
                n_splits=n_splits, concurrency=concurrency, headless=headless, scroll_ratio=scroll_ratio)

all_results = scweet.scrape(since="2024-07-01", until="2025-07-01", words=['Pepecoin'], to_account=None, from_account=None,
                              lang="en", limit=500,
                              display_type="Top", resume=False, filter_replies=False, proximity=False,
                              geocode=None, minreplies=10, minlikes=10, minretweets=10, save_dir='outputs',
                            custom_csv_name='pepe.csv')