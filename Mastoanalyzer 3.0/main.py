import requests
import mysql.connector
import logging
import sys
import os
import time 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graphix import main_graphix_user, main_graphix_post, tempo_di_risposta, plot_user_stats
from credentials import access_token, instance_url, host, user, password, database
from FetchAll import get_timeline_posts, debug
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

def main():
    """
    The main function to run the program. Displays a menu for users to interact with different features.
    Features include:
        - Fetching and storing timeline posts in the database.
        - Retrieving data from the database.
        - Detecting bots.
        - Generating user and post graphs.
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
    
    
    proxy_list = [] #fill 
    
    try:
        while True:
            print("Menu:")
            print("1. Catch em' all")
            print("2. Catch post' all")
            print("3. Find bots")
            print("4. Genera grafico utenti")
            print("5. Genera grafico post")
            print("6. Mostra i dati sugli utenti reali")
            print("q. Exit")

            choice = input("Pick an option (1/2/3/4/5/6/q): ").strip().lower()

            match choice:
                case '1':
                    start = time.time()
                    all_users, array_tempo_di_risposta = get_timeline_posts(public_timeline_url, params, headers, db_connection)
                    end = time.time()
                    diff = end-start
                    logging.info(f"Time for requests: {diff}")
                    tempo_di_risposta(array_tempo_di_risposta)
                    break
                case '2':
                    users = get_data_from_table(db_connection, "users") 
                    start = time.time()
                    array_tempo_di_risposta = debug(users, db_connection, proxy_list)
                    end = time.time()
                    diff = end-start
                    print(array_tempo_di_risposta)
                    print()
                    logging.info(f"Time for requests: {diff}")
                    tempo_di_risposta(array_tempo_di_risposta)
                case '3':
                    sospettati, numero = find_bot(db_connection)
                    logging.info(f"Number of suspicious users: {num}, these are: {sos}")
                    break
                case '4':
                    main_graphix_user(db_connection)
                    break
                case '5':
                    main_graphix_post(db_connection)
                case '6':
                    plot_user_stats(db_connection)
                case 'q':
                    print("Leave the program.")
                    break
                case _:
                    print("Invalid option. Please choose 1/2/3/4/5/6 or q.")
                
    finally:
        db_connection.close()  

if __name__ == '__main__':
    main()
