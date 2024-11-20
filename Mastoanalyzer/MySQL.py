#query the table
import asyncio
import httpx
import aiomysql
import mysql.connector

def get_user(db_connection):
    """
    Retrieves all user records from the 'users' table in the database.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - list: A list of dictionaries, where each dictionary represents a user record.
    """
    cursor = db_connection.cursor(dictionary=True)
    query = """ 
    SELECT * 
    FROM users
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_user_posts(db_connection): 
    """
    Retrieves all posts from the 'posts' table and their corresponding users from the 'users' table in the database.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - list: A list of dictionaries, where each dictionary represents a post and its associated user.
    """
    cursor = db_connection.cursor(dictionary=True)
    query = """ 
    SELECT * 
    FROM posts AS p
    JOIN users AS u ON p.account_id = u.user_id
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_utenti_pubblicazione(db_connection):
    """
    Retrieves the average posting interval (in seconds) for each non-bot user in the 'mastodon.posts' table.
    It calculates the time difference between consecutive posts for each user.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - list: A list of dictionaries, where each dictionary contains a username and their average posting interval.
    """
    cursor = db_connection.cursor(dictionary=True)
    query = """ 
    WITH differenze_temporali AS (
    SELECT
        account_username,
        created_at,
        LAG(created_at) OVER (PARTITION BY account_username ORDER BY created_at) AS post_precedente
    FROM mastodon.posts AS p
    JOIN mastodon.users AS u ON p.account_id = u.user_id
    WHERE u.bot = 0
    ORDER BY account_username ASC
    ), q AS (
    SELECT
        account_username,
        AVG(TIMESTAMPDIFF(SECOND, post_precedente, created_at)) AS intervallo_medio_secondi
    FROM
        differenze_temporali
    WHERE
        post_precedente IS NOT NULL
    GROUP BY
        account_username
    )
    SELECT * FROM q
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_bot_pubblicazione(db_connection):
    """
    Retrieves the average posting interval (in seconds) for each bot user in the 'mastodon.posts' table.
    It calculates the time difference between consecutive posts for each bot.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - list: A list of dictionaries, where each dictionary contains a bot username and their average posting interval.
    """
    cursor = db_connection.cursor(dictionary=True)
    query = """ 
    WITH differenze_temporali AS (
    SELECT
        account_username,
        created_at,
        LAG(created_at) OVER (PARTITION BY account_username ORDER BY created_at) AS post_precedente
    FROM mastodon.posts AS p
    JOIN mastodon.users AS u ON p.account_id = u.user_id
    WHERE u.bot = 1
    ORDER BY account_username ASC
    ), q AS (
    SELECT
        account_username,
        AVG(TIMESTAMPDIFF(SECOND, post_precedente, created_at)) AS intervallo_medio_secondi
    FROM
        differenze_temporali
    WHERE
        post_precedente IS NOT NULL
    GROUP BY
        account_username
    )
    SELECT * FROM q
    """
    cursor.execute(query)
    return cursor.fetchall()

def del_user(url, db_connection):
    """
    Deletes a user record from the 'users' table based on the user's URL.
    
    Parameters:
    - url (str): The URL of the user to be deleted.
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    """
    cursor = db_connection.cursor()

    # Extracts the user ID from the URL
    IDuser = url.split('/')[6]

    cursor.execute(
    """
        DELETE FROM users
        WHERE user_id = %s;
    """, 
    (IDuser,)
    )
    db_connection.commit() 
    cursor.close()

def create_bots_users_table(all_users, db_connection):
    """
    Inserts new bot user data into the 'users' table or updates existing records 
    if the bot user already exists based on the user ID.
    
    Parameters:
    - all_users (list): A list of tuples containing user data (user_id, username, bot, url, followers, following, statuses, description).
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    """
    cursor = db_connection.cursor() 

    for user in all_users:
        cursor.execute(
    """
    INSERT INTO users (user_id, username, bot, url, followers, following, statuses, description)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        bot = VALUES(bot),
        url = VALUES(url),
        followers = VALUES(followers),
        following = VALUES(following),
        statuses = VALUES(statuses),
        description = VALUES(description)
    """,
    user 
    )

    db_connection.commit() 
    cursor.close()

def get_data_from_table(db_connection, table, offset, limit):
    """
    Retrieves a specified range of user data from a given table where the number of statuses is greater than 200.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    - table (str): The table name from which to fetch the data.
    - offset (int): The offset for pagination, determining the starting point.
    - limit (int): The number of records to retrieve.
    
    Returns:
    - list: A list of dictionaries containing user data (user_id, username, bot, url, followers, following, statuses, description).
    """
    cursor = db_connection.cursor()
    cursor.execute(f"SELECT user_id, username, bot, url, followers, following, statuses, description FROM {table} WHERE statuses > 200 ORDER BY statuses DESC LIMIT {limit} OFFSET {offset}")
    return cursor.fetchall()

def create_post_per_user(db_connection, tupla_post):
    """
    Inserts a list of posts into the 'posts' table in the database. If a post already exists (duplicate key), 
    it updates the existing post with the new data.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    - tupla_post (list): A list of tuples, where each tuple contains the data for a single post.
    """
    cursor = db_connection.cursor()

    for tupla in tupla_post:
        try:
            cursor.execute(
            """
            INSERT INTO posts (
            post_id, 
            created_at, 
            in_reply_to_id, 
            in_reply_to_account_id, 
            `sensitive`, 
            `spoiler_text`, 
            `visibility`, 
            `language`, 
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
            media_attachments, 
            account_id, 
            account_username, 
            account_display_name, 
            account_url, 
            reblog_id, 
            reblog_content, 
            reblogged_from_account
            ) VALUES (
                %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, 
                %s, %s, %s, %s, 
                %s
            ) ON DUPLICATE KEY UPDATE
                `created_at` = VALUES(`created_at`),
                `in_reply_to_id` = VALUES(`in_reply_to_id`),
                `in_reply_to_account_id` = VALUES(`in_reply_to_account_id`),
                `sensitive` = VALUES(`sensitive`),
                `spoiler_text` = VALUES(`spoiler_text`),
                `visibility` = VALUES(`visibility`),
                `language` = VALUES(`language`),
                uri = VALUES(uri),
                url = VALUES(url),
                replies_count = VALUES(replies_count),
                reblogs_count = VALUES(reblogs_count),
                favourites_count = VALUES(favourites_count),
                favourited = VALUES(favourited),
                reblogged = VALUES(reblogged),
                muted = VALUES(muted),
                bookmarked = VALUES(bookmarked),
                pinned = VALUES(pinned),
                content = VALUES(content),
                media_attachments = VALUES(media_attachments),
                account_username = VALUES(account_username),
                account_display_name = VALUES(account_display_name),
                account_url = VALUES(account_url),
                reblog_id = VALUES(reblog_id),
                reblog_content = VALUES(reblog_content),
                reblogged_from_account = VALUES(reblogged_from_account);
            """, 
            tupla 
            )
        except Exception as e:
            print(f"Error: {e}")

    db_connection.commit()
    cursor.close()

def get_all_user(db_connection):
    """
    Retrieves all non-bot user records from the 'users' table in the database.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - list: A list of dictionaries, where each dictionary represents a non-bot user record.
    """
    cursor = db_connection.cursor(dictionary=True)
    query = """ 
    SELECT * 
    FROM users
    WHERE bot = 0
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_all_bot(db_connection):
    """
    Retrieves all bot user records from the 'users' table in the database.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - list: A list of dictionaries, where each dictionary represents a bot user record.
    """
    cursor = db_connection.cursor(dictionary=True)
    query = """ 
    SELECT * 
    FROM users
    WHERE bot = 1
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_user_no_bot(db_connection):
    """
    Retrieves non-bot users from the 'users' table who have more than 200 statuses, 
    ordered by the number of statuses in descending order.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - list: A list of dictionaries, where each dictionary represents a non-bot user with more than 200 statuses.
    """
    cursor = db_connection.cursor(dictionary=True)
    query = """
        SELECT *
        FROM users 
        WHERE bot = 0 
        AND statuses > 200 
        ORDER BY statuses DESC;
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_pubblicazione(user, db_connection):
    """
    Retrieves the average posting interval (in seconds) for a specific non-bot user based on their posting history.
    
    Parameters:
    - user (str): The username of the user whose posting interval is to be calculated.
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - list: A list containing the user's average posting interval in seconds.
    """
    cursor = db_connection.cursor(dictionary=True)
    query = """ 
    WITH differenze_temporali AS (
        SELECT
            account_username,
            created_at,
            LAG(created_at) OVER (PARTITION BY account_username ORDER BY created_at) AS post_precedente
        FROM mastodon.posts AS p
        JOIN mastodon.users AS u ON p.account_id = u.user_id
        WHERE u.bot = 0 AND u.username = %s
        ORDER BY account_username ASC
    ),
    q AS (
        SELECT
            account_username,
            AVG(TIMESTAMPDIFF(SECOND, post_precedente, created_at)) AS intervallo_medio_secondi
        FROM differenze_temporali
        WHERE post_precedente IS NOT NULL
        GROUP BY account_username
    )
    SELECT * FROM q
    """
    try: 
        cursor.execute(query, (user,))
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Error: {e}")
        return [0]
    finally:
        cursor.close()

def post_per_user(user, db_connection):
    """
    Retrieves the posts of a specific user from the database.
    
    Parameters:
    - user (str): The username of the user whose posts are to be retrieved.
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - list: A list of dictionaries representing the user's posts.
    """
    cursor = db_connection.cursor(dictionary=True)
    query = """ 
    SELECT * 
    FROM posts 
    WHERE account_username = %s
    """
    cursor.execute(query, (user,))
    return cursor.fetchall()


    cursor = db_connection.cursor(dictionary=True)
    query = """
        SELECT * FROM posts 
        WHERE account_id = %s;
    """
    cursor.execute(query, (user_id,))
    return cursor.fetchall()