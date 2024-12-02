import requests
import mysql.connector
import logging
import sys
import os
import time 
import asyncio 
import httpx
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)
from graphix import main_graphix_user, main_graphix_post, tempo_di_risposta, plot_user_stats
from credentials import access_token, instance_url, host, user, password, database
from FetchAll import get_timeline_posts, async_debug
from MySQL import create_bots_users_table, get_data_from_table
from FindBot import find_bot

def connect_to_db(): 
    """
    Establish a connection to the MySQL database.
    Returns the connection object if successful, or None if an error occurs.
    """
    try: 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            db=database
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

async def async_main():
    """
    Main asynchronous function that provides a menu-driven interface
    for interacting with the database and performing various tasks.
    """
    primary_tag = 'politics'
    additional_tags = ['technology', 'science', 'internet']
    public_timeline_url = f"{instance_url}/api/v1/timelines/tag/{primary_tag}"
    params = {
        'any': additional_tags
    }
   
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
   
    db_connection = connect_to_db()
    if not db_connection:
        logging.error("Database connection failed. Exiting...")
        return
    
    try:
        while True:
            # Display menu options
            print("Menu:")
            print("1. Fetch all users")
            print("2. Fetch posts for all users")
            print("3. Find bots")
            print("4. Generate user graphs")
            print("5. Generate post graphs")
            print("6. Show data for users")
            print("q. Exit")

            choice = input("Pick an option (1/2/3/4/5/6/q): ").strip().lower()

            match choice:
                case '1':
                    start = time.time()
                    all_users, array_tempo_di_risposta = get_timeline_posts(public_timeline_url, params, headers, db_connection)
                    end = time.time()
                    diff = end - start
                    logging.info(f"Time for requests: {diff}")
                    tempo_di_risposta(array_tempo_di_risposta)
                    break
                case '2':
                    # Fetch data for different user groups
                    users_group_1 = get_data_from_table(db_connection, "users", 0, 100)
                    users_group_2 = get_data_from_table(db_connection, "users", 100, 100)
                    users_group_3 = get_data_from_table(db_connection, "users", 200, 100)
                    
                    array_tempo_di_risposta = await handle_case_2(db_connection, users_group_1, users_group_2, users_group_3)
                    
                    # Display response times for each group
                    tempo_di_risposta(array_tempo_di_risposta[0])
                    tempo_di_risposta(array_tempo_di_risposta[1])
                    tempo_di_risposta(array_tempo_di_risposta[2])
                    
                case '3':
                    # Find suspicious accounts
                    sos, num = find_bot(db_connection)
                    logging.info(f"Number of suspicious users: {num}, these are: {sos}")
                    break
                case '4':
                    # Generate user-related graphs
                    main_graphix_user(db_connection)
                    break
                case '5':
                    # Generate post-related graphs
                    main_graphix_post(db_connection)
                case '6':
                    # Show statistics for real users
                    plot_user_stats(db_connection)
                case 'q':
                    print("Exiting the program.")
                    break
                case _:
                    print("Invalid option. Please choose 1/2/3/4/5/6 or q.")
    finally:
        db_connection.close()

async def handle_case_2(db_connection, users_group_1, users_group_2, users_group_3):
    """
    Handle case 2: Fetch data for users using multiple proxies and measure response times.
    """
    start = time.time()
    array_tempo_di_risposta = await run_debug_tasks(users_group_1, users_group_2, users_group_3, db_connection)
    end = time.time()
    diff = end - start
    print(array_tempo_di_risposta[0]) # Each numeric value represents the time to query one user, i.e., the time for 5 requests.
    print()
    print(array_tempo_di_risposta[1])
    print()
    print(array_tempo_di_risposta[2])
    logging.info(f"Time for requests (handle_case_2): {diff}")
    return array_tempo_di_risposta

async def run_debug_tasks(users_group_1, users_group_2, users_group_3, db_connection):
    """
    Create asynchronous tasks to debug user groups using an HTTP client.
    """
    tasks = []
    users_groups = [users_group_1, users_group_2, users_group_3]

    async with httpx.AsyncClient() as client:
        for users in users_groups:
            # Create an asynchronous task for each group
            task = asyncio.create_task(async_debug(users, db_connection, client))
            tasks.append(task)

        # Wait for all tasks to complete and collect their results
        responses = await asyncio.gather(*tasks)
    return responses

if __name__ == "__main__":
    asyncio.run(async_main())