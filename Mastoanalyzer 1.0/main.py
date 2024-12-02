import requests
import mysql.connector
import logging
import sys
import os
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graphix import main_graphix_user, main_graphix_post
from credentials import access_token, instance_url, host, user, password, database
from FetchAll import get_timeline_posts, debug
from MySQL import create_bots_users_table, get_data_from_table
from FindBot import find_bot

def create_db_connection(host, user, password, database):
    """
    Establishes a connection to the MySQL database.
    
    Args:
        host (str): The database host address.
        user (str): The username for the database.
        password (str): The password for the database user.
        database (str): The name of the database to connect to.
    
    Returns:
        mysql.connector.connection.MySQLConnection: The database connection object.
    
    Raises:
        SystemExit: If the connection fails, the program exits with an error message.
    """
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        logging.info("Database connection established successfully.")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to the database: {err}")
        sys.exit(1)

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

    db_connection = create_db_connection(host, user, password, database)

    try:
        while True:
            print("Menu:")
            print("1. Catch em' all")
            print("2. Catch post' all")
            print("3. Find bots")
            print("4. Generate user graph")
            print("5. Generate post graph")
            print("q. Exit")

            choice = input("Pick an option (1/2/3/4/5/q): ").strip().lower()

            match choice:
                case '1':
                    all_users = get_timeline_posts(public_timeline_url, params, headers)
                    create_bots_users_table(all_users, db_connection)
                    break
                case '2':
                    users = get_data_from_table(db_connection, "users", 0, 10)
                    debug(users, db_connection)
                case '3':
                    find_bot(db_connection)
                    break
                case '4':
                    main_graphix_user(db_connection)
                    break
                case '5':
                    main_graphix_post(db_connection)
                case 'q':
                    print("Exiting the program.")
                    break
                case _:
                    print("Invalid option. Please choose 1/2/3/4/5 or q.")
    finally:
        db_connection.close()

if __name__ == '__main__':
    main()
