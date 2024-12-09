import requests
import logging
import sys
import os
import json
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from credentials import access_token, instance_url, host, user, password, database
from Ausiliario import handle_http_error, time_to_sleep, format_content, format_datetime
from MySQL import del_user, get_data_from_table, create_post_per_user


def get_timeline_posts(public_timeline_url, params, headers):
    """
    Fetches posts from the public timeline, processes user data, and handles rate limits.

    Args:
        public_timeline_url (str): The URL of the public timeline.
        params (dict): Query parameters for the request.
        headers (dict): Headers for the request.

    Returns:
        set: A set of tuples containing user data.
    """

    logging.info(f"Fetching posts from URL: {public_timeline_url}")
    stop = 6000
    max_id = None  
    all_users = set() 
    session = requests.Session() 
    
    try:
        session.headers.update(headers)  
        params.update({'limit': 40})  

        attempt = 0 
        max_retries = 3 

        while attempt < max_retries:
            if max_id:
                params['max_id'] = max_id 

            try:
                response = session.get(public_timeline_url, params=params)
                rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))

                try:
                    posts = response.json()
                except json.JSONDecodeError: # per evitare Expecting value: line 2 column 5 (char 5)
                    logging.error(f"Failed to decode JSON for URL {url}. Response content: {response.content}")
                    continue
                if not posts:
                    break

                for post in posts:
                    try:
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
                        logging.error(f"Campo mancante nella risposta del post: {e}")
                        continue  
                    except TypeError as e:
                        logging.error(f"Errore di tipo con il post: {e}")
                        continue
                    if 'bsky.brid.gy' in url: #strange instance
                        continue 
                    else:
                        all_users.add((user_id, username, bot, url, followers, following, statuses, description))      
                if len(all_users) > stop:
                    break

                logging.info(f"Lunghezza di all_users: {lau}")
                logging.info(f"Rate Limit Remaining: {rate_limit_remaining}")
                print()
                
                max_id = posts[-1]['id'] # Ottieni l'ID dell'ultimo post per la pagina successiva

            except requests.exceptions.HTTPError as e:
                status_code = handle_http_error(response)
                logging.error(f"Errore HTTP {e} per l'utente: {url}")
                if status_code == '429':
                    attempt += 1
                    logging.info(f"Tentativo n. {attempt} di max {max_retries} ")
                    time_to_sleep() 
                    continue 

                elif status_code == '503':
                    logging.error(f"Errore HTTP {e} per l'utente: {url}")
                    time_to_sleep() 
                    continue 

            except Exception as err:
                logging.error(f"Other error occurred: {err}")
                logging.error(f"Failed to retrieve posts. Then fall asleep.")
                time_to_sleep() # automatizzo implementando time sleep            
                continue

    finally:
        session.close()  
        logging.info(f"Total users fetched: {len(all_users)}") 
        return all_users

def fetch_posts(url, statuses, db_connection, session, max_retries):
    """
    Fetches posts for a specific user from the provided URL and processes them.

    Args:
        url (str): The URL to fetch posts from.
        statuses (int): Total number of posts expected from the user.
        db_connection: Database connection object for performing user deletions.
        session (requests.Session): The session object for making HTTP requests.
        max_retries (int): The maximum number of retry attempts for handling connection issues.

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
                
                response = session.get(url, params=params) 
                response.raise_for_status()
                posts = response.json() 
                if not posts:
                    logging.warning(f"Nessun post trovato per l'utente: {url}")
                    
                    
                    logging.info("Post inesistenti, elimino utente dalla tabella")
                    del_user(url, db_connection)
                    break

                all_posts.extend(posts) 
                params['max_id'] = int(posts[-1]['id']) - 1 
                break 

            except requests.exceptions.HTTPError as e:
                status_code = handle_http_error(response)
                logging.error(f"Errore HTTP {e} per l'utente: {url}")
                if status_code == '429':
                    time_to_sleep() 
                    continue 
                
                
                logging.info("Errore di fetch post, elimino utente dalla tabella users.")
                del_user(url, db_connection)
                
                break 
            
            except requests.exceptions.ConnectionError as e:
                logging.warning(f"Tentativo {attempt + 1} fallito per {url} per connessione abortita: {e}.")
                attempt += 1
                if attempt < max_retries:
                    tts = time_to_sleep()
                    time.sleep(tts) 
                else:
                    logging.error(f"Connessione fallita per {url} dopo {max_retries} tentativi.")
                    return [], 0 

    
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

    
    return tupla_post, total_requests  

def debug(bots, db_connection):
    """
    Queries user data and fetches their posts for debugging and analysis purposes.

    Args:
        bots (list): A list of tuples representing bot/user records, where each tuple contains:
            - bot_id (str): The ID of the bot/user.
            - username (str): The username of the bot/user.
            - is_bot (bool): Whether the account is identified as a bot.
            - url (str): The profile URL of the bot/user.
            - followers (int): The number of followers the bot/user has.
            - following (int): The number of accounts the bot/user is following.
            - statuses (int): The total number of posts/statuses of the bot/user.
            - description (str): The description or bio of the bot/user.
        db_connection: Database connection object used to insert posts and perform database operations.

    Returns:
        list: A list of durations (in seconds) for each user's post-fetching process.

    Notes:
        - Fetches posts for each user from the specified API URL.
        - Measures the time taken for each user's data retrieval and logs the total requests and users queried.
        - Calls `fetch_posts` to retrieve posts and `create_post_per_user` to insert the data into the database.
    """
    
    print("Interrogazione degli utenti...")
    print()

    durata = []
    richieste, utenti = 0, 0
    max_retries=3
    session = requests.Session()

    for bot in bots:

        bot_id, username, is_bot, url, followers, following, statuses, description = bot #considero tutto il record
        url = f"https://mastodon.social/api/v1/accounts/{bot_id}/statuses" #url per prendere i post
        print(f"Interrogando {username} - URL: {url} - statuses: {statuses}") #per debug 

        start = time.time()
        users_posts, total_requests = fetch_posts(url, statuses, db_connection, session,max_retries)
        end = time.time()
        diff = round((end-start),2)
        durata.append(diff)
        
        create_post_per_user(db_connection, users_posts) #aggiungo i post alla tabella
        richieste += total_requests
        utenti +=1
        print(durata)
        logging.info('Richieste totali effettuate fino ad ora: %d', richieste)
        logging.info('Utenti interrogati fino ad ora: %d', utenti)
        print()

    return durata
    cursor.close()