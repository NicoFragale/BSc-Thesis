# Contains functions that generate graphs for comparison between real users and bots
import matplotlib.pyplot as plt
import seaborn as sns
from statistics import median, mean
from MySQL import get_user, get_user_posts, get_utenti_pubblicazione, get_bot_pubblicazione, get_all_user
import numpy as np
from sklearn.linear_model import LinearRegression

def plot_metric_comparison(user_data, bot_data, metric_name, ax, user_color, bot_color, ylabel, xlabel, median_user, median_bot, mean_user, mean_bot):
    """
    This function generates a density plot comparing a specific metric (e.g., followers, username length) 
    between real users and bots. It includes lines for the mean and median of both groups.
    """

    # Define the chart characteristics, two types, the color of each, fill below the curve, density, style, name, and position
    sns.histplot(user_data, color=user_color, fill=True, label="User Data", alpha=0.4,  kde = True)
    # Grafico Bot
    sns.histplot(bot_data, color=bot_color, fill=True, label="Bot Data", alpha=0.4, kde = True)


    # Lines for median and mean
    ax.axvline(median_user, color=user_color, linestyle='--', linewidth=1.5, label=f'Median Users: {median_user:.2f}')
    ax.axvline(median_bot, color=bot_color, linestyle='--', linewidth=1.5, label=f'Median Bots: {median_bot:.2f}')
    ax.axvline(mean_user, color=user_color, linestyle='-', linewidth=1.5, label=f'Mean Users: {mean_user:.2f}')
    ax.axvline(mean_bot, color=bot_color, linestyle='-', linewidth=1.5, label=f'Mean Bots: {mean_bot:.2f}')
    
    # Settings
    ax.set_title(f'{metric_name} - Comparison between Users and Bots', color='black', fontsize=18)
    ax.set_ylabel(ylabel, fontsize=14)  # Show the density estimate of popularity
    ax.set_xlabel(xlabel, fontsize=14)
    plt.xticks(fontsize=12)  
    plt.yticks(fontsize=12) 
    ax.legend(fontsize=14)  # Legend
    ax.grid(True)  # Grid in the chart

def main_graphix_user(db_connection): 
    """
    This function generates graphs comparing various user statistics (e.g., username length, followers) 
    between real users and bots using data retrieved from the database.
    """
    print("Running main_graphix...")  

    # Retrieve all users
    all_users = get_user(db_connection)

    # If the database is empty
    if not all_users:
        print("None data")
        return

    # Lists to store data for graphs
    usernameUser, usernameBot = [], []
    followersUser, followersBot = [], []
    followingUser, followingBot = [], []
    statusesUser, statusesBot = [], []
    descriptionUser, descriptionBot = [], [] 
    
    for user in all_users:
        if user['bot'] == 0:  # Real user
            usernameUser.append(len(user['username']))
            followersUser.append(user['followers'])
            followingUser.append(user['following'])
            statusesUser.append(user['statuses'])
            descriptionUser.append(len(user['description']))  
        elif user['bot'] == 1:  # Bot
            usernameBot.append(len(user['username']))
            followersBot.append(user['followers'])
            followingBot.append(user['following'])
            statusesBot.append(user['statuses'])
            descriptionBot.append(len(user['description']))  # Added description length for bots

    # Calculate median and mean for each parameter
    median_len_user, median_len_bot = median(usernameUser), median(usernameBot)
    mean_len_user, mean_len_bot = mean(usernameUser), mean(usernameBot)

    median_followers_user, median_followers_bot = median(followersUser), median(followersBot)
    mean_followers_user, mean_followers_bot = mean(followersUser), mean(followersBot)

    median_following_user, median_following_bot = median(followingUser), median(followingBot)
    mean_following_user, mean_following_bot = mean(followingUser), mean(followingBot)

    median_statuses_user, median_statuses_bot = median(statusesUser), median(statusesBot)
    mean_statuses_user, mean_statuses_bot = mean(statusesUser), mean(statusesBot)

    median_description_user, median_description_bot = median(descriptionUser), median(descriptionBot)
    mean_description_user, mean_description_bot = mean(descriptionUser), mean(descriptionBot)

    # Subplot, i.e., 5 graphs (5 rows and 1 column) with size 10x25
    fig, axes = plt.subplots(1, 1, figsize=(10, 25))  # Added the fifth graph
    user_color = '#1f77b4'  
    bot_color = '#ff7f0e'
    
    stampa_cose(usernameUser, usernameBot, followersUser, followersBot, followingUser, followingBot, statusesUser, statusesBot, descriptionUser, descriptionBot)
    
    # Generate the plots, one at a time. To plot multiple graphs, change the number in line 81 to (x, 1)
    plot_metric_comparison(usernameUser, usernameBot, "Username length", axes, user_color, bot_color,'Users', "Length", median_len_user, median_len_bot, mean_len_user, mean_len_bot)
    #plot_metric_comparison(followersUser, followersBot, "Number of Followers", axes, user_color, bot_color, 'Users',"Followers", median_followers_user, median_followers_bot, mean_followers_user, mean_followers_bot)
    #plot_metric_comparison(followingUser, followingBot, "Number of Following", axes, user_color, bot_color, 'Users', "Following", median_following_user, median_following_bot, mean_following_user, mean_following_bot)
    #plot_metric_comparison(statusesUser, statusesBot, "Number of Statuses", axes, user_color, bot_color, 'Users',"Statuses", median_statuses_user, median_statuses_bot, mean_statuses_user, mean_statuses_bot)
    #plot_metric_comparison(descriptionUser, descriptionBot, "Description Length", axes, user_color, bot_color, 'Users',"Description Length", median_description_user, median_description_bot, mean_description_user, mean_description_bot)

    plt.xticks(fontsize=12)  
    plt.yticks(fontsize=12) 
    plt.tight_layout()
    plt.show()
    print("Done")  
    
def main_graphix_post(db_connection): 
    """
    This function generates graphs comparing various post metrics (e.g., reply count, reblog count) 
    between real users and bots using data retrieved from the database.
    """
    print("Running main_graphix...")  

    # Retrieve all posts
    all_posts = get_user_posts(db_connection)

    # If the database is empty
    if not all_posts:
        print("None")
        return

    timePubUser, timePubBot = [], []
    replyUser, replyBot = [], []
    reblogUser, reblogBot = [], []
    favCountUser, favCountBot = [], []
    
    timePubUserQuery = get_utenti_pubblicazione(db_connection)
    timePubBotQuery = get_bot_pubblicazione(db_connection)

    for x in timePubUserQuery:
        timePubUser.append(x['intervallo_medio_secondi'])

    for x in timePubBotQuery:
        timePubBot.append(x['intervallo_medio_secondi'])

    for post in all_posts:
        if post['bot'] == 0:
            replyUser.append(post['replies_count'])
            reblogUser.append(post['reblogs_count'])
            favCountUser.append(post['favourites_count'])
        elif post['bot'] == 1:
            replyBot.append(post['replies_count'])
            reblogBot.append(post['reblogs_count'])
            favCountBot.append(post['favourites_count'])
    
    median_timePubUser, median_timePubBot = median(timePubUser), median(timePubBot)
    mean_timePubUser, mean_timePubBot = mean(timePubUser), mean(timePubBot)

    median_replyUser, median_replyBot = median(replyUser), median(replyBot)
    mean_replyUser, mean_replyBot = mean(replyUser), mean(replyBot)

    median_reblogUser, median_reblogBot = median(reblogUser), median(reblogBot)
    mean_reblogUser, mean_reblogBot = mean(reblogUser), mean(reblogBot)
    
    median_favCountUser, median_favCountBot = median(favCountUser), median(favCountBot)
    mean_favCountUser, mean_favCountBot = mean(favCountUser), mean(favCountBot)

    # Subplot, i.e., 4 graphs (4 rows and 1 column) with size 10x25
    fig, axes = plt.subplots(1, 1, figsize=(10, 25))  # Added the fifth graph
    user_color = '#1f77b4'  
    bot_color = '#ff7f0e'

    # Generate the plots, one at a time. To plot multiple graphs, change the number in line 150 to (x)
    plot_metric_comparison(timePubUser, timePubBot, "Posting Interval", axes, user_color, bot_color, "Users", "Seconds", median_timePubUser, median_timePubBot, mean_timePubUser, mean_timePubBot)
    #plot_metric_comparison(replyUser, replyBot, "Replies Count", axes[1], user_color, bot_color, "Replies", median_replyUser, median_replyBot, mean_replyUser, mean_replyBot)
    #plot_metric_comparison(reblogUser, reblogBot, "Reblogs Count", axes[2], user_color, bot_color, "Reblogs", median_reblogUser, median_reblogBot, mean_reblogUser, mean_reblogBot)
    #plot_metric_comparison(favCountUser, favCountBot, "Favorites Count", axes[3], user_color, bot_color, "Favorites", median_favCountUser, median_favCountBot, mean_favCountUser, mean_favCountBot)

    plt.xticks(fontsize=12)  
    plt.yticks(fontsize=12) 
    plt.tight_layout()
    plt.show()
    print("Done")

# Function to print median and mean values for various user and bot statistics
def stampa_cose(usernameUser, usernameBot, followersUser, followersBot, followingUser, followingBot, statusesUser, statusesBot, descriptionUser, descriptionBot):
    """
    This function prints the median and mean values for various metrics (username length, followers count, etc.) 
    for both real users and bots.
    """
    # Print median and mean for username lengths
    print('Median username length')
    print(median(usernameUser))
    print(median(usernameBot))
    print('Mean username length')
    print(mean(usernameUser))
    print(mean(usernameBot))
    print()

    # Print median and mean for followers count
    print('Median followers')
    print(median(followersUser))
    print(median(followersBot))
    print('Mean followers')
    print(mean(followersUser))
    print(mean(followersBot))
    print()
    
    # Print median and mean for following count
    print('Median following')
    print(median(followingUser))
    print(median(followingBot))
    print('Mean following')
    print(mean(followingUser))
    print(mean(followingBot))
    print()

    # Print median and mean for statuses count
    print('Median statuses')
    print(median(statusesUser))
    print(median(statusesBot))
    print('Mean statuses')
    print(mean(statusesUser))
    print(mean(statusesBot))
    print()

    # Print median and mean for description length
    print('Median description length')
    print(median(descriptionUser))
    print(median(descriptionBot))
    print('Mean description length')
    print(mean(descriptionUser))
    print(mean(descriptionBot))

# Function to plot the response time over iterations
def tempo_di_risposta(array):
    """
    This function plots the response time as a function of iterations using a line graph.
    It helps visualize how the response time increases as the number of iterations grows.
    """
    m = len(array) + 1  # Total number of data points (adding 1 to ensure proper x-axis length)
    x = np.arange(1, m)  # Create an array of x values representing the iterations
    plt.figure(figsize=(10, 6))  # Set the figure size
    
    # Plot the response time (y values) against the iterations (x values)
    plt.plot(x, array, label="Request time", color='b', linestyle='-', marker=' ') 

    # Add title and axis labels
    plt.title('Increase in Request Time', fontsize=18)
    plt.xlabel('Requests', fontsize=14)
    plt.ylabel('Request time (seconds)', fontsize=14)
    
    # Display grid, legend, and set font sizes for ticks
    plt.grid(True) 
    plt.legend(fontsize=14)  
    plt.xticks(fontsize=12)  
    plt.yticks(fontsize=12)   

    # Display the plot
    plt.show()

def plot_user_stats(db_connection):
    #prendo gli utenti
    real_users = get_all_user(db_connection)
    sorted_users = sorted(real_users, key=lambda user: user['statuses'], reverse=True)
    statuses = [user['statuses'] for user in sorted_users]
    num_users = list(range(1, len(sorted_users) + 1))

    followers = [user['followers'] for user in real_users]
    following = [user['following'] for user in real_users]
    

    #media e mediana per tutti e 3 i grafici
    ers_mean = np.mean(followers)
    ers_median = np.median(followers)

    ing_mean = np.mean(following)
    ing_median = np.median(following)

    ses_mean = np.mean(statuses)
    ses_median = np.median(statuses)
    
    # Crea un istogramma dei followers
    plt.figure(figsize=(14, 8))
    sns.histplot(statuses, bins=30, kde=True, color='#1f77b4', alpha=0.7, edgecolor="black", label="Number of Statuses")

    # Linee per media e mediana
    plt.axvline(ses_mean, color='red', linestyle='--', linewidth=2, label=f'Mean: {ses_mean:.2f}')
    plt.axvline(ses_median, color='green', linestyle='-', linewidth=2, label=f'Median: {ses_median:.2f}')

    # Dettagli del grafico
    plt.title("Distribution of User Statuses", fontsize=18)
    plt.xlabel("Number of Statuses", fontsize=14)
    plt.ylabel("Users", fontsize=14)
    plt.legend(fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(True, linestyle='-', alpha=0.6)
    plt.show()

