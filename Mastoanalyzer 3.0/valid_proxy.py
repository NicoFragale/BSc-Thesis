import requests
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from credentials import access_token

def validate_proxies(proxy_list, endpoint_url):
    """
    Validates a list of proxy servers by testing them against a given endpoint.

    Parameters:
    - proxy_list (list): A list of proxy server URLs to be validated.
    - endpoint_url (str): The API endpoint to test the proxies against.

    Returns:
    - valid_proxies (list): A list of proxies that successfully connect to the endpoint.
    
    Functionality:
    - Iterates through the provided list of proxies and attempts to make a GET request to the specified endpoint.
    - Uses the `access_token` for authorization in the request headers.
    - Proxies that successfully return a response within the timeout period are added to the `valid_proxies` list.
    - Prints the status of each proxy (success or failure) and the number of proxies left to validate.
    - Handles exceptions gracefully, logging errors for failed proxies and continuing with the remaining ones.
    """
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    valid_proxies = []  # List to store valid proxies, fill the list
    remaining_proxies = len(proxy_list)  # Counter for remaining proxies
    print(f"Total proxies to validate: {remaining_proxies}")

    for proxy in proxy_list:
        try:
            # Attempt to use the proxy to connect to the endpoint
            response = requests.get(
                endpoint_url,
                headers=headers,
                proxies={'http': proxy, 'https': proxy},
                timeout=15
            )
            print(f"Proxy {proxy} worked successfully.")
            valid_proxies.append(proxy)  # Add the working proxy to the list
        except Exception as e:
            # Log the error for the failed proxy
            print(f"Error with proxy {proxy}: {e}")
        finally:
            # Decrement the counter for remaining proxies and display the count
            remaining_proxies -= 1
            print(f"Proxies left to validate: {remaining_proxies}")
            print()

    print(f"Valid proxies: {valid_proxies}")
    return valid_proxies
