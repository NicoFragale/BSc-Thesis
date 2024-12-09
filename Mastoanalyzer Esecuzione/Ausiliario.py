# Contains auxiliary functions
import logging
import sys
import os
import time
import requests
import httpx
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime, timezone
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from credentials import access_token

def format_datetime(iso_datetime):
    """
    Converts an ISO 8601 datetime string into a format compatible with MySQL DATETIME.
    
    Parameters:
    - iso_datetime (str): A datetime string in ISO 8601 format.
    
    Returns:
    - str: A formatted datetime string in "YYYY-MM-DD HH:MM:SS".
    """
    try:
        # Parse ISO 8601 datetime string, ignoring milliseconds and 'Z'
        dt = datetime.strptime(iso_datetime, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Handles cases without milliseconds
        dt = datetime.strptime(iso_datetime, "%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
        
def format_content(content):
    """
    Cleans HTML content by removing all tags and extracting plain text.
    
    Parameters:
    - content (str): A string containing HTML content.
    
    Returns:
    - str: The plain text extracted from the HTML.
    """
    soup = BeautifulSoup(content, 'html.parser')
    return soup.get_text()

def time_to_sleep(): 
    """
    Pauses execution to respect the rate limit of the API.
    
    It checks the 'X-RateLimit-Reset' header to determine when the rate limit resets.
    If the header is not present or cannot be parsed, a default wait time of 300 seconds is used.
    
    Returns:
    - int: The time (in seconds) the program will sleep.
    """
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.get('https://mastodon.social/api/v1/accounts/verify_credentials', headers=headers)
    except requests.exceptions.RequestException as e:
        # If there's a connection error, wait for 300 seconds
        logging.warning("Connection error. Waiting for 300 seconds before retrying.")
        time.sleep(300)
        return 300

    rate_limit_reset = response.headers.get('X-RateLimit-Reset')

    if rate_limit_reset:
        # Convert ISO 8601 format to datetime with UTC offset
        reset_time = datetime.fromisoformat(rate_limit_reset.replace('Z', '+00:00'))
        
        # Convert to UTC for comparison
        now_utc = datetime.now(timezone.utc)
        
        # Calculate remaining time until the rate limit reset
        time_until_reset = (reset_time - now_utc).total_seconds()
        
        logging.info(f"Rate limit reached. Sleeping for {time_until_reset} seconds.")
        print()
        time.sleep(max(0, time_until_reset))
    else:
        # Default wait time
        logging.warning("Sleeping for 300 seconds by default.")
        time_until_reset = 300
        time.sleep(time_until_reset)

    return time_until_reset
    
def handle_http_error(response):
    """
    Handles common HTTP error codes by logging appropriate error messages.
    
    Parameters:
    - response (requests.Response): The HTTP response object to analyze.
    
    Returns:
    - str: The status code of the response as a string.
    """
    status_code = response.status_code
    match status_code:
        case 400:
            logging.error("Bad Request: The server could not understand the request.")
        case 401:
            logging.error("Unauthorized: Authentication is required and has failed or has not been provided.")
        case 403:
            logging.error("Forbidden: The server understood the request but refuses to authorize it.")
        case 404:
            logging.error("Not Found: The requested resource could not be found.")
        case 500:
            logging.error("Internal Server Error: The server encountered an internal error and was unable to complete your request.")
        case 429:
            logging.error("Rate limit exceeded. Waiting before retrying.")
        case _:
            logging.error(f"HTTP error occurred with status code {status_code}: {response.reason}")
    return str(status_code)
