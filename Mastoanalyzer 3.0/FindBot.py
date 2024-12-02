from MySQL import get_all_bot, get_user_no_bot, get_bot_pubblicazione, get_pubblicazione, post_per_user
import mysql.connector
import logging
import numpy as np
from statistics import median, mean
import time  
import re
from datetime import datetime

def median_calculator(db_connection):
    """
    Calculates the median values for various user metrics such as followers, following, 
    description length, statuses, and average publication intervals for bots in the database.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - tuple: The median values for followers, following, description length, statuses, and average posting interval.
    """
    bots = get_all_bot(db_connection)
    followers = []
    following = []
    description = []
    statuses = []
    timePubBot = []
    for bot in bots:
        followers.append(bot['followers'])
        following.append(bot['following'])
        description.append(len(bot['description']))
        statuses.append(bot['statuses'])

    timePubBotQuery = get_bot_pubblicazione(db_connection)
    for x in timePubBotQuery:
        timePubBot.append(x['intervallo_medio_secondi'])

    ers_median, ing_median, desc_median, statuses_median, fr_median = median(followers), median(following), median(description), median(statuses), median(timePubBot)
    return ers_median, ing_median, desc_median, statuses_median, fr_median

def check_ersing(followers, following, ers_median, ing_median):
    """
    Checks if the followers and following counts of a user are within acceptable tolerance limits 
    compared to the medians of bot followers and following counts.
    
    Parameters:
    - followers (int): The number of followers of the user.
    - following (int): The number of users the user is following.
    - ers_median (float): The median value for followers in bot data.
    - ing_median (float): The median value for following in bot data.
    
    Returns:
    - bool: True if the user's metrics are within the tolerance range of bot medians, False otherwise.
    """
    tolleranza = 10 
    tolleranzaErs = 20
    # tolerance limits, around 10%
    ers_lower_bound = ers_median * (1 - tolleranzaErs / 100)
    ers_upper_bound = ers_median * (1 + tolleranzaErs / 100)
    ing_lower_bound = ing_median * (1 - tolleranza / 100)
    ing_upper_bound = ing_median * (1 + tolleranza / 100)
    if ers_lower_bound <= followers <= ers_upper_bound or ing_lower_bound <= following <= ing_upper_bound:
        return True
    else:
        return False

def check_descriptionLength(description, desc_median):
    """
    Checks if the length of a user's description is within acceptable tolerance limits 
    compared to the median description length of bots. It also checks if the description contains a link.
    
    Parameters:
    - description (str): The description of the user.
    - desc_median (float): The median length of descriptions in bot data.
    
    Returns:
    - bool: True if the description length is within tolerance or if it contains a link, False otherwise.
    """
    tolleranza = 10 
    # tolerance limits, around 10%
    lower_bound = desc_median * (1 - tolleranza / 100)
    upper_bound = desc_median * (1 + tolleranza / 100)
    # checks if the description contains a link
    link_pattern = r"(https?://[^\s]+|www\.[^\s]+)"  # checks if there is https... or www...
    bool = re.search(link_pattern, description) is not None

    if lower_bound <= len(description) <= upper_bound or bool is True:
        return True
    else:
        return False

def check_statuses(statuses, statuses_median):
    """
    Checks if the number of statuses a user has is within acceptable tolerance limits 
    compared to the median number of statuses of bots.
    
    Parameters:
    - statuses (int): The number of statuses the user has.
    - statuses_median (float): The median number of statuses in bot data.
    
    Returns:
    - bool: True if the number of statuses is within tolerance, False otherwise.
    """
    tolleranza = 10 
    # tolerance limits, around 10%
    lower_bound = statuses_median * (1 - tolleranza / 100)
    upper_bound = statuses_median * (1 + tolleranza / 100)
    if lower_bound <= statuses <= upper_bound:
        return True
    else:
        return False

def check_outlier(statuses):
    """
    Checks if the number of statuses is an outlier (too high).
    
    Parameters:
    - statuses (int): The number of statuses the user has.
    
    Returns:
    - bool: True if the number of statuses is considered an outlier, False otherwise.
    """
    x = 80000  # threshold based on calculated graphs
    if statuses > x:
        return True
    else:
        return False

def check_intervalPosting(user, fr_median, db_connection):
    """
    Checks if the posting interval for a user is within acceptable tolerance limits 
    compared to the median posting interval of bots.
    
    Parameters:
    - user (str): The username of the user.
    - fr_median (float): The median posting interval for bots.
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - bool: True if the user's posting interval is within tolerance, False otherwise.
    """
    timePubUser = []
    timePubUserQuery = get_pubblicazione(user, db_connection)  # fetches the user's posting interval from the database
    if len(timePubUserQuery) == 0:  # in case no posts are in the table
        timePubUser.append(0)  # as median needs a non-empty list
    else:
        for x in timePubUserQuery:
            timePubUser.append(x['intervallo_medio_secondi'])

    user_fr_median = median(timePubUser)
    tolleranza = 10 
    # tolerance limits, around 10%
    lower_bound = float(fr_median) * (1 - tolleranza / 100)
    upper_bound = float(fr_median) * (1 + tolleranza / 100)
    if lower_bound <= user_fr_median <= upper_bound:
        return True
    else:
        return False

def find_bot(db_connection):
    """
    Identifies users who may be bots based on several criteria such as followers, following, 
    description length, status count, and posting frequency. Users are flagged as bots if 
    they meet certain conditions based on the median values of bot data.
    
    Parameters:
    - db_connection (MySQL Connection): The connection object to interact with the MySQL database.
    
    Returns:
    - tuple: A list of suspicious users (username, URL) and the count of suspicious users.
    """
    no_bots = get_user_no_bot(db_connection)
    ers_median, ing_median, desc_median, statuses_median, fr_median = median_calculator(db_connection)
    user_sospetti = []
    for nb in no_bots: 
        start = time.time()
        ersing = check_ersing(nb['followers'], nb['following'], ers_median, ing_median)
        desc = check_descriptionLength(nb['description'], desc_median)
        statuses = check_statuses(nb['statuses'], statuses_median)
        outlier = check_outlier(nb['statuses'])
        interval = check_intervalPosting(nb['username'], fr_median, db_connection)

        check_finale = [ersing, desc, statuses, interval]
        print(check_finale)
        true_count = sum(check_finale)
        if true_count >= 2 or outlier == True: 
            user_sospetti.append((nb['username'], nb['url']))
    debug(start)
    return user_sospetti, len(user_sospetti)

def debug(start): 
    """
    Logs the start and end time of the bot detection process, and the duration it took.
    
    Parameters:
    - start (float): The timestamp when the bot detection process started.
    """
    end = time.time()
    diff = round((end - start), 2)
    start_time_formatted = datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S')
    end_time_formatted = datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Research for bot started at: {start_time_formatted} and ended at {end_time_formatted}, it took {diff} seconds")
