import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

RAPID_API_KEY = os.getenv("RAPID_API_KEY")
API_HOST = "nairobi-stock-exchange-nse.p.rapidapi.com"
API_URL = "https://nairobi-stock-exchange-nse.p.rapidapi.com/stocks"


def test_api():
    if not all([RAPID_API_KEY, API_HOST, API_URL]):
        print("Missing API configuration in .env")
        return

    headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": API_HOST
    }

    try:
        print("Sending request...\n")

        response = requests.get(
            API_URL,
            headers=headers,
            timeout=30
        )

        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print("\n--- API RESPONSE ---\n")

        try:
            data = response.json()
            print(json.dumps(data, indent=4, ensure_ascii=False))

        except ValueError:
            print(response.text)

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


if __name__ == "__main__":
    test_api()