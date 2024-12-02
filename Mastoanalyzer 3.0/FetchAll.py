import requests
import logging
import sys
import os
import json
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from credentials import access_token, instance_url, host, user, password, database
from Ausiliario import handle_http_error, time_to_sleep, format_content, format_datetime
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
    stop = 7000
    tempo_di_risposta = []  # The list contains all response times; used to generate the time efficiency chart.
    max_id = None  # To track the captured posts.
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

def fetch_posts(url, statuses, db_connection, max_retries, proxy_list, session, current_proxy_index):
    """
    Fetches posts for a specific user from the provided URL and processes them.

    Args:
        url (str): The URL to fetch posts from.
        statuses (int): Total number of posts expected from the user.
        db_connection: Database connection object for performing user deletions.
        session (requests.Session): The session object for making HTTP requests.
        max_retries (int): The maximum number of retry attempts for handling connection issues.
        proxy_list (list):  List of proxy used to query the server
        current_proxy_index (int): Index for proxy_list

    Returns:
        tuple: A list of tuples containing post data and the total number of requests made.
    """

    all_posts = [] 
    statuses_per_request = 40 
    params = {"limit": statuses_per_request}
    n_richieste = 5 

    total_requests = min((statuses // statuses_per_request) + (statuses % statuses_per_request > 0), n_richieste)

    for _ in range(total_requests): 
        
        attempt = 0 
        while attempt < max_retries:  
            
            try:
                if current_proxy_index == -1:
                    response = session.get(url, params=params) 
                else:
                    response = session.get(url, params=params, timeout=15)

                response.raise_for_status() 
                posts = response.json() 
                if not posts: 

                    logging.warning(f"{proxy_list[current_proxy_index]} - Nessun post trovato per l'utente: {url}")
                    logging.info(f"{proxy_list[current_proxy_index]} - Post inesistenti, elimino utente dalla tabella")
                    del_user(url, db_connection) 
                    return [], 0, current_proxy_index

                all_posts.extend(posts) 
                params['max_id'] = int(posts[-1]['id']) - 1 
                break 
            
            except requests.exceptions.HTTPError as e:

                status_code = handle_http_error(response)
                logging.error(f"{proxy_list[current_proxy_index]} - Errore HTTP {e} per l'utente: {url}")
                if status_code == '429': 
                    current_proxy_index = (current_proxy_index + 1) % (len(proxy_list)+1) 
                    if current_proxy_index == len(proxy_list): 
                        session.proxies = None 
                        logging.info("Usando IP locale per la richiesta.")
                        print("Proxy attivo per la richiesta: Nessun proxy (IP locale)")
                        current_proxy_index = -1 
                    else: 
                        session.proxies = {"http": proxy_list[current_proxy_index], "https": proxy_list[current_proxy_index]}
                        logging.info(f"Cambio proxy: {proxy_list[current_proxy_index]}")
                        print(f"Proxy attivo per la richiesta: {session.proxies}")
                        print()
                        print()
                        print()
                        continue

                logging.info(f"{proxy_list[current_proxy_index]} - Errore di fetch post, elimino utente dalla tabella users.")
                del_user(url, db_connection)
                break 

            except requests.exceptions.ProxyError as e:
                
                current_proxy_index = (current_proxy_index + 1) % (len(proxy_list)+1)
                logging.error(f"Proxy error: {e}.")
                if current_proxy_index == len(proxy_list): 
                    session.proxies = None
                    print()
                    logging.info("Usando IP locale per la richiesta.")
                    print("Proxy attivo per la richiesta: Nessun proxy (IP locale)")
                    current_proxy_index = -1
                else: 
                    session.proxies = {"http": proxy_list[current_proxy_index], "https": proxy_list[current_proxy_index]}
                    logging.info(f"Cambio proxy: {proxy_list[current_proxy_index]}")
                    print(f"Proxy attivo per la richiesta: {session.proxies}")
                    print()
                    print()
                    print()
                    continue 
            
            except requests.exceptions.ReadTimeout as e:
                logging.warning(f"Timeout al tentativo {attempt + 1} per {url}: {e}")
                attempt += 1
                if attempt < max_retries:
                    current_proxy_index = (current_proxy_index + 1) % (len(proxy_list)+1)
                    if current_proxy_index == len(proxy_list): 
                        session.proxies = None
                        logging.info("Usando IP locale per la richiesta.")
                        print("Proxy attivo per la richiesta: Nessun proxy (IP locale)")
                        current_proxy_index = -1
                    else:
                        session.proxies = {"http": proxy_list[current_proxy_index], "https": proxy_list[current_proxy_index]}
                        logging.info(f"Cambio proxy: {proxy_list[current_proxy_index]}")
                        print(f"Proxy attivo per la richiesta: {session.proxies}")
                        print()
                        print()
                        print()
                else:
                    logging.error(f"Connessione fallita per {url} dopo {max_retries} tentativi.")
                    return [], 0, current_proxy_index
            
            except requests.exceptions.ConnectionError as e:
                logging.warning(f"Tentativo {attempt + 1} fallito per {url} per connessione abortita: {e}.")
                attempt += 1
                if attempt < max_retries:
                    current_proxy_index = (current_proxy_index + 1) % (len(proxy_list)+1)
                    if current_proxy_index == len(proxy_list): 
                        session.proxies = None
                        logging.info("Usando IP locale per la richiesta.")
                        print("Proxy attivo per la richiesta: Nessun proxy (IP locale)")
                        current_proxy_index = -1
                    else:
                        session.proxies = {"http": proxy_list[current_proxy_index], "https": proxy_list[current_proxy_index]}
                        logging.info(f"Cambio proxy: {proxy_list[current_proxy_index]}")
                        print(f"Proxy attivo per la richiesta: {session.proxies}")
                        print()
                        print()
                        print()
                             
                else:
                    logging.error(f"Connessione fallita per {url} dopo {max_retries} tentativi.")
                    return [], 0, current_proxy_index

            except ChunkedEncodingError as chunk_err:
                current_proxy_index = (current_proxy_index + 1) % (len(proxy_list)+1) 
                print(f"{proxy_list[current_proxy_index]} - Errore di trasmissione dei dati: {chunk_err}")
                if current_proxy_index == len(proxy_list):
                    session.proxies = None
                    logging.info("Usando IP locale per la richiesta.")
                    print("Proxy attivo per la richiesta: Nessun proxy (IP locale)")
                    current_proxy_index = -1
                else: 
                    session.proxies = {"http": proxy_list[current_proxy_index], "https": proxy_list[current_proxy_index]}
                    logging.info(f"Cambio proxy: {proxy_list[current_proxy_index]}")
                    print(f"Proxy attivo per la richiesta: {session.proxies}")
                    print()
                    print()
                    print()
                    continue 
    
    tupla_post = []
    for post in all_posts:

        
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
        
        if reblog is not None:
            reblog_id = reblog.get('id')
            reblog_content = reblog.get('content', '')
            max_text_length = 65535
        
            if len(reblog_content) > max_text_length:
                reblog_content = reblog_content[:max_text_length]  
            reblogged_from_account = reblog.get('account', {}).get('username')
        else: 
            reblog = None
            reblog_id = None
            reblogged_from_account = None
            reblog_content = None

        tupla_post.append((
            post_id, 
            created_at,
            in_reply_to_id,
            in_reply_to_account_id, 
            sensitive, 
            spoiler_text, 
            visibility,
            language, 
            uri, 
            url, 
            replies_count, 
            reblogs_count, 
            favourites_count, 
            favourited, 
            reblogged, 
            muted,
            bookmarked, 
            pinned, 
            content, 
            json.dumps(media_attachments), 
            account_id, 
            account_username,
            account_display_name, 
            account_url, 
            reblog_id, 
            reblog_content, 
            reblogged_from_account
        ))

    return tupla_post, total_requests, current_proxy_index  

def debug(users, db_connection, proxy_list):
    """
    Queries user data and fetches their posts for debugging and analysis purposes.

    Args:
        users (list): A list of tuples representing bot/user records, where each tuple contains:
            - bot_id (str): The ID of the bot/user.
            - username (str): The username of the bot/user.
            - is_bot (bool): Whether the account is identified as a bot.
            - url (str): The profile URL of the bot/user.
            - followers (int): The number of followers the bot/user has.
            - following (int): The number of accounts the bot/user is following.
            - statuses (int): The total number of posts/statuses of the bot/user.
            - description (str): The description or bio of the bot/user.
        db_connection: Database connection object used to insert posts and perform database operations.
        proxy_list: List of proxy used to query the server
        
    Returns:
        tempo_di_risposta: A list of durations (in seconds) for each user's post-fetching process.

    Notes:
        - Fetches posts for each user from the specified API URL.
        - Measures the time taken for each user's data retrieval and logs the total requests and users queried.
        - Calls `fetch_posts` to retrieve posts and `create_post_per_user` to insert the data into the database.
    """
    session = requests.Session() 
    current_proxy_index = -1 
    print("Querying users...")
    print()
    i = 0  
    u = 0  
    max_retries = 3 
    tempo_di_risposta = []
    
    for user in users:
        user_id, username, is_bot, url, followers, following, statuses, description = user 
        url = f"https://mastodon.social/api/v1/accounts/{user_id}/statuses" 
        print(f"Interrogando {username} - URL: {url} - statuses: {statuses}") 
        
        start = time.time()
        users_posts, total_requests, proxy = fetch_posts(url, statuses, db_connection, max_retries, proxy_list, session, current_proxy_index) 
        current_proxy_index = proxy 
        end = time.time() 
        diff = end - start
        diff = round(diff, 3) 
        tempo_di_risposta.append(diff)
        logging.info(f"{proxy_list[current_proxy_index]} - Tempo di risposta per {url}: {diff} secondi") 
        
        create_post_per_user(db_connection, users_posts) 
        
        i += total_requests
        u+=1
        logging.info('Richieste totali effettuate fino ad ora: %d', i)
        logging.info('Utenti interrogati fino ad ora: %d', u)
        print()
    
    session.close()
    return tempo_di_risposta 