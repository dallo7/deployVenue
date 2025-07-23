import requests
import json
import urllib.parse


BASE_URL = "http://127.0.0.1:5060"


def get_venue_report(venue_name):
    """
    Calls the Flask Venue Analytics API to get the venue report for a given venue name.
    """
    # URL-encode the venue name to handle spaces and special characters correctly in the URL
    encoded_venue_name = urllib.parse.quote(venue_name)

    endpoint = f"/venue_report/{encoded_venue_name}"
    url = f"{BASE_URL}{endpoint}"

    print(f"Attempting to fetch report for venue: '{venue_name}' from {url}")

    try:
        response = requests.get(url)

        # Raise an HTTPError for bad responses (4xx or 5xx status codes)
        response.raise_for_status()

        # Parse the JSON response from the API
        data = response.json()

        if data.get("status") == "success":
            print(f"\n✅ Successfully retrieved report for '{venue_name}':")
            print(json.dumps(data.get("data"), indent=4))
        else:
            print(f"\n❌ Failed to retrieve report for '{venue_name}':")
            # Print the entire error response
            print(json.dumps(data, indent=4))

    except requests.exceptions.ConnectionError as e:
        print(f"\nConnection Error: Could not connect to the Flask API at {BASE_URL}.")
        print(f"Please ensure your Flask Venue Analytics API (venue_app.py) is running.")
        print(f"Error details: {e}")
    except requests.exceptions.HTTPError as e:
        print(f"\nHTTP Error: Received a bad response from the server.")
        print(f"Status Code: {e.response.status_code}")
        print(f"Response: {e.response.text}")
    except json.JSONDecodeError:
        print(f"\nError: Could not decode JSON response from the server.")
        print(f"Raw Response: {response.text}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")


if __name__ == "__main__":

    print("--- Testing with 'RocoMamas Kenya' ---")
    get_venue_report("RocoMamas Kenya")

    print("\n" + "="*60 + "\n")

    print("--- Testing with 'The Carnivore' ---")
    get_venue_report("The Carnivore")

    print("\n" + "="*60 + "\n")

    print("--- Testing with a non-existent venue ---")
    get_venue_report("Imaginary Venue Hall")

    print("\n" + "="*60 + "\n")

    print("--- Testing with another existing venue (case insensitive example) ---")
    get_venue_report("the carnivore")
