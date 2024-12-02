#contiene le funzioni che fanno domande al server per ottenere i dati(utenti e post)
import requests
import logging
import sys
import os
import json
import time
import asyncio 
import httpx
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from credentials import access_token, instance_url, host, user, password, database
from Ausiliario import handle_http_error, time_to_sleep, format_content, format_datetime, time_to_sleep_async
from MySQL import del_user, get_data_from_table, create_post_per_user, create_bots_users_table
from requests.exceptions import ChunkedEncodingError, HTTPError

def get_timeline_posts(public_timeline_url, params, headers, db_connection):
    """
    Function Purpose:
    This function retrieves posts from a public Mastodon timeline API endpoint and processes them to extract user details.
    It handles rate limits, manages server response times, and stores extracted user data into a database.

    Parameters:
    - public_timeline_url (str): The URL of the Mastodon public timeline API.
    - params (dict): Query parameters for the API requests (e.g., filters, pagination).
    - headers (dict): HTTP headers for the requests (e.g., authentication tokens, user-agent).
    - db_connection: The database connection object for storing processed user data.

    Returns:
    - list of all_users (list): A list of tuples, where each tuple contains extracted user details.
      Each tuple has the format: 
      (user_id, username, bot, url, followers, following, statuses, description)
    - tempo_di_risposta (list): A list of response times (in seconds) for each API request, 
      useful for generating efficiency metrics and diagnostics.
    
    Key Features:
    - Handles paginated data using `max_id` to fetch posts beyond the initial set.
    - Processes posts to extract and format user details while ensuring robust error handling for missing or malformed fields.
    - Tracks server response times and logs them for performance monitoring.
    - Implements rate-limit handling and optional sleep mechanisms to avoid exceeding API limits.
    - Uses a session object for efficiency when making multiple requests.
    """
    logging.info(f"Fetching posts from URL: {public_timeline_url}")
    tempo_di_risposta = []  # The list contains all response times; used to generate the time efficiency chart.
    max_id = None  # To track the captured posts.
    stop = 7000
    all_users = set()  # To create the table of all users in the timeline.
    session = requests.Session()  # Open a single session for 300 requests.
    contatore = 0  # Counter to track the number of requests made.
    while True:
        try:
            session.headers.update(headers)  # Define session headers.
            params.update({'limit': 40})  # Number of posts per request.
            if max_id:
                params['max_id'] = max_id  # Update the ID to navigate to the next page.

            start = time.time()  # To measure the server's response time.
            response = session.get(public_timeline_url, params=params)  # Request to the timeline.
            end = time.time()  # Measure the end time for server response.
            diff = end - start  # Calculate the response time.
            diff = round(diff, 3)
            tempo_di_risposta.append(diff)
            contatore += 1
            # Remaining rate limit
            rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))

            try:
                posts = response.json()  # List of dictionaries representing posts.
            except json.JSONDecodeError:  # To handle errors like "Expecting value: line 2 column 5 (char 5)".
                logging.error(f"Failed to decode JSON for URL {url}.")
                continue

            if not posts:  # If the request returns an empty response.
                break  # End the program execution.

            # Process each post
            for post in posts:
                try:
                    # Since errors like "Expecting value: line 2 column 5 (char 5)" occasionally occur,
                    # set default values (e.g., 'N/A', False, 0) in case fields are missing.
                    # This is enclosed in try-except to handle KeyError or TypeError.
                    username = post['account'].get('username', 'N/A')
                    user_id = post['account'].get('id', 'N/A')
                    bot = post['account'].get('bot', False)
                    url = post['account'].get('url', 'N/A')
                    followers = post['account'].get('followers_count', 0)
                    following = post['account'].get('following_count', 0)
                    statuses = post['account'].get('statuses_count', 0)
                    description = post['account'].get('note', 'N/A')
                    description = format_content(description)

                except KeyError as e:
                    logging.error(f"Field missing: {e}")
                    continue  # Skip the incomplete or missing post.
                except TypeError as e:
                    logging.error(f"Error for post: {e}")
                    continue

                if 'bsky.brid.gy' in url:  # An ambiguous instance returning incorrect info.
                    continue
                else:
                    all_users.add((user_id, username, bot, url, followers, following, statuses, description))

            create_bots_users_table(all_users, db_connection)

            # A mechanism to pause the program after collecting a certain number of users.
            if len(all_users) > stop:
                break  # End program execution.
            if contatore % 500 == 0:
                contatore = 0
                logging.info(f"500 requests reached, pausing until reset time.")
                time_to_sleep()

            print(tempo_di_risposta)
            logging.info(f"Length of all_users: {len(all_users)}")
            logging.info(f"Rate Limit Remaining: {rate_limit_remaining}")
            logging.info(f"Request time: {diff} sec")
            print()

            # Get the ID of the last post to move to the next page.
            max_id = posts[-1]['id']

        except requests.exceptions.HTTPError as e:
            status_code = handle_http_error(response)
            logging.error(f"HTTP error {e} for user: {url}")

            if status_code in ['429', '503']:
                time_to_sleep()
                continue

        except Exception as err:
            logging.error(f"Other error occurred: {err}")
            logging.error(f"Failed to retrieve the response object.")
            time_to_sleep()  # Automate with sleep time.
            continue

    session.close()  # Close the session.
    logging.info(f"Total users fetched: {len(all_users)}")  # Unique users participating in the timeline.
    return list(all_users), tempo_di_risposta

async def fetch_posts_async(url, statuses, db_connection, max_retries, client):
    """
    Function Purpose:
    This function retrieves posts from a specified Mastodon API endpoint for a given user.
    It handles rate limits, network errors, and proxies, ensuring robust data fetching.

    Parameters:
    - url (str): The API endpoint to fetch posts for a specific user.
    - statuses (int): Total number of statuses (posts) available for the user.
    - db_connection: The database connection used to update or remove user data.
    - max_retries (int): Maximum number of retries allowed for a request in case of failures.
    - proxy_list (list): List of proxy servers to use for making requests.
    - client (httpx.AsyncClient): The asynchronous HTTP client used for requests.
    - current_proxy_index (int): Index of the currently used proxy in the proxy list.

    Returns:
    - tupla_post (list of tuples): A list of tuples containing the extracted and formatted post data.
    - total_requests (int): Total number of requests made for fetching posts.
    - current_proxy_index (int): Updated index of the proxy used after function execution.
    """

    all_posts = []  # Container for all posts fetched for the user.
    statuses_per_request = 40  # Number of posts fetched per request.
    params = {"limit": statuses_per_request}  # Parameters for the API request.
    n_richieste = 5  # Maximum number of requests per user.
    attempt = 0  # Counter for connection attempts.
    # Calculate the number of requests required based on available posts.
    total_requests = min((statuses // statuses_per_request) + (statuses % statuses_per_request > 0), n_richieste)

    for _ in range(total_requests):  # Loop to perform the required number of requests.
        while attempt < max_retries:  # Retry in case of timeout or connection errors.
            try:
                # Set the proxy for the client and make the request.
                response = await client.get(url, params=params, timeout=15)
                response.raise_for_status()  # Check for HTTP status (e.g., 200 OK).
                posts = response.json()  # Extract posts from the response.
                
                try:
                    posts = response.json()
                except json.JSONDecodeError: # per evitare Expecting value: line 2 column 5 (char 5)
                    logging.error(f"Failed to decode JSON for URL {url}. Response content: {response.content}")
                    continue

                if not posts:  # If no posts are found for the user.
                    del_user(url, db_connection)  # Remove user from the database.
                    return [], 0 # Stop processing for this user.

                all_posts.extend(posts)  # Append the fetched posts to the list.
                params['max_id'] = int(posts[-1]['id']) - 1  # Update parameter for next request.
                break  # Exit retry loop if successful.

            except httpx.HTTPStatusError as e:  # Handle HTTP errors (e.g., 429, 503).
                status_code = handle_http_error(response)
                if status_code == '429':  # If rate limit is exceeded.
                    await time_to_sleep_async()
                    continue

                del_user(url, db_connection)  # Remove user if fetching fails.
                break  # Exit retry loop.


            except Exception as e:  # Handle any other unexpected errors.
                return [], 0

    # Format and structure the retrieved data for database insertion.
    tupla_post = []
    for post in all_posts:
        # Extract relevant fields from the post JSON.
        post_id = post.get('id')
        created_at = format_datetime(post.get('created_at'))
        in_reply_to_id = post.get('in_reply_to_id')
        in_reply_to_account_id = post.get('in_reply_to_account_id')
        sensitive = post.get('sensitive', False)
        spoiler_text = post.get('spoiler_text', '')
        visibility = post.get('visibility', 'public')
        language = post.get('language', '')
        uri = post.get('uri')
        url = post.get('url')
        replies_count = post.get('replies_count', 0)
        reblogs_count = post.get('reblogs_count', 0)
        favourites_count = post.get('favourites_count', 0)
        favourited = post.get('favourited', False)
        reblogged = post.get('reblogged', False)
        muted = post.get('muted', False)
        bookmarked = post.get('bookmarked', False)
        pinned = post.get('pinned', False)
        content = format_content(post.get('content', ''))
        media_attachments = post.get('media_attachments', [])

        account = post.get('account', {})
        account_id = account.get('id')
        account_username = account.get('username')
        account_display_name = account.get('display_name', '')
        account_url = account.get('url')

        reblog = post.get('reblog', {})
        if reblog:
            reblog_id = reblog.get('id')
            reblog_content = reblog.get('content', '')[:65535]  # Truncate content if too long.
            reblogged_from_account = reblog.get('account', {}).get('username')
        else:
            reblog_id = None
            reblog_content = None
            reblogged_from_account = None

        # Append extracted data to the result list.
        tupla_post.append((
            post_id, created_at, in_reply_to_id, in_reply_to_account_id, sensitive, spoiler_text, visibility, language,
            uri, url, replies_count, reblogs_count, favourites_count, favourited, reblogged, muted, bookmarked, pinned,
            content, json.dumps(media_attachments), account_id, account_username, account_display_name, account_url,
            reblog_id, reblog_content, reblogged_from_account
        ))

    return tupla_post, total_requests  # Return structured data and updated proxy index.

async def async_debug(users, db_connection, client):
    """
    Function Purpose:
    This function serves as an asynchronous task to query user information concurrently. 
    It uses proxies to balance the load and fetch data about users' posts from a Mastodon instance.
    
    Parameters:
    - users: List of user records containing user data (ID, username, etc.).
    - db_connection: Database connection for storing user post data.
    - proxy_list: List of proxies for balancing requests and avoiding rate limits.

    Returns:
    - tempo_di_risposta: A list of response times for each request.

    Notes:
        - Fetches posts for each user from the specified API URL.
        - Measures the time taken for each user's data retrieval and logs the total requests and users queried.
        - Calls `fetch_posts` to retrieve posts and `create_post_per_user` to insert the data into the database.
    """
    
    i = 0  # Counter for the total number of requests made so far.
    u = 0  # Counter for the total number of users queried so far.
    max_retries = 3  # Number of retries allowed in case of an error.
    tempo_di_risposta = []  # List to store all response times.

    logging.info("Querying users...")
    for user in users:

        # Decompose the user record into individual fields.
        user_id, username, is_bot, url, followers, following, statuses, description = user
        # Construct the URL to fetch posts for the user.
        url = f"https://mastodon.social/api/v1/accounts/{user_id}/statuses"

        start = time.time()  # Start measuring the time for data retrieval.
        # Perform an asynchronous request to fetch posts for the user.
        users_posts, total_requests = await fetch_posts_async(
            url, statuses, db_connection, max_retries, client
        )
        
        end = time.time()  # End time measurement.
        diff = round(end - start, 3)  # Calculate the response time.
        tempo_di_risposta.append(diff)  # Add the response time to the list.
        

        # Add the fetched posts to the database table.
        create_post_per_user(db_connection, users_posts)

        i += total_requests  # Increment the total requests counter.
        u += 1  # Increment the total users queried counter.
        logging.info(f"Finished querying {u} users with {i} requests.")


    return tempo_di_risposta  # Return the list of response times.