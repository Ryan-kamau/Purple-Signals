# app/services/news_service.py

import os
from typing import Optional

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class NewsService:
    """
    Service for interacting with the NewsAPI top headlines endpoint.
    """

    BASE_URL = "https://newsapi.org/v2/top-headlines"

    def __init__(self):
        self.api_key = os.getenv("NEWS_API_KEY")

        if not self.api_key:
            raise ValueError(
                "NEWS_API_KEY not found in environment variables."
            )

    def get_top_headlines(
        self,
        country: str = "us",
        category: Optional[str] = None,
        query: Optional[str] = None,
        sources: Optional[str] = None,
        page_size: int = 10,
        page: int = 1,
    ):
        """
        Fetch top headlines from NewsAPI.

        Args:
            country: 2-letter ISO country code
            category: business, sports, technology, etc.
            query: keyword search
            sources: comma-separated source IDs
            page_size: number of results per page
            page: page number

        Returns:
            Dictionary response from NewsAPI
        """

        params = {
            "apiKey": self.api_key,
            "pageSize": page_size,
            "page": page,
        }

        # NewsAPI rules:
        # You cannot mix `sources` with `country` or `category`
        if sources:
            params["sources"] = sources
        else:
            params["country"] = country

            if category:
                params["category"] = category

        if query:
            params["q"] = query

        try:
            response = requests.get(
                self.BASE_URL,
                params=params,
                timeout=10,
            )

            response.raise_for_status()

            data = response.json()

            if data.get("status") != "ok":
                return {
                    "success": False,
                    "error": data.get("message", "Unknown API error"),
                }

            return {
                "success": True,
                "total_results": data.get("totalResults", 0),
                "articles": data.get("articles", []),
            }

        except requests.exceptions.Timeout:
            return {
                "success": False,
                "error": "Request timed out.",
            }

        except requests.exceptions.HTTPError as e:
            return {
                "success": False,
                "error": f"HTTP error: {str(e)}",
            }

        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": f"Request failed: {str(e)}",
            }
        
    EVERYTHING_URL = "https://newsapi.org/v2/everything"

    def search_everything(
        self,
        query: str,
        search_in: Optional[str] = None,
        sources: Optional[str] = None,
        domains: Optional[str] = None,
        exclude_domains: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        language: Optional[str] = None,
        sort_by: str = "publishedAt",
        page_size: int = 20,
        page: int = 1,
    ):
        """
        Search through all articles using NewsAPI Everything endpoint.

        Args:
            query:
                Search keywords or advanced query.
                Examples:
                    "bitcoin"
                    '"artificial intelligence"'
                    '+bitcoin -ethereum'
                    'crypto AND (bitcoin OR ethereum)'

            search_in:
                Restrict search fields.
                Options:
                    title
                    description
                    content
                Example:
                    "title,content"

            sources:
                Comma-separated source IDs.

            domains:
                Restrict results to specific domains.
                Example:
                    "bbc.co.uk,techcrunch.com"

            exclude_domains:
                Exclude specific domains.

            from_date:
                Oldest allowed article date.
                Example:
                    "2026-05-26"

            to_date:
                Newest allowed article date.

            language:
                2-letter ISO language code.
                Example:
                    "en"

            sort_by:
                Options:
                    relevancy
                    popularity
                    publishedAt

            page_size:
                Max 100.

            page:
                Pagination page number.

        Returns:
            Dictionary response from NewsAPI.
        """

        params = {
            "apiKey": self.api_key,
            "q": query,
            "sortBy": sort_by,
            "pageSize": page_size,
            "page": page,
        }

        if search_in:
            params["searchIn"] = search_in

        if sources:
            params["sources"] = sources

        if domains:
            params["domains"] = domains

        if exclude_domains:
            params["excludeDomains"] = exclude_domains

        if from_date:
            params["from"] = from_date

        if to_date:
            params["to"] = to_date

        if language:
            params["language"] = language

        try:
            response = requests.get(
                self.EVERYTHING_URL,
                params=params,
                timeout=15,
            )

            response.raise_for_status()

            data = response.json()

            if data.get("status") != "ok":
                return {
                    "success": False,
                    "error": data.get("message", "Unknown API error"),
                }

            return {
                "success": True,
                "total_results": data.get("totalResults", 0),
                "articles": data.get("articles", []),
            }

        except requests.exceptions.Timeout:
            return {
                "success": False,
                "error": "Request timed out.",
            }

        except requests.exceptions.HTTPError as e:
            return {
                "success": False,
                "error": f"HTTP error: {str(e)}",
            }

        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": f"Request failed: {str(e)}",
            }


if __name__ == "__main__":
    news_service = NewsService()

    # Example 1: Business headlines
    business_news = news_service.get_top_headlines(
        category="business",
        sources="Thenation",
        page_size=10,
    )

    print("\n=== BUSINESS NEWS ===\n")

    if business_news["success"]:
        for article in business_news["articles"]:
            print(f"Title: {article['title']}")
            print(f"Source: {article['source']['name']}")
            print(f"Published: {article['publishedAt']}")
            print(f"URL: {article['url']}")
            print(f"Content: {article['content']}")
            print("-" * 80)
    else:
        print("Error:", business_news["error"])

    # Example 2: Search headlines
    trump_news = news_service.get_top_headlines(
        query="Trump",
        page_size=3,
    )

    print("\n=== TRUMP NEWS ===\n")

    if trump_news["success"]:
        for article in trump_news["articles"]:
            print(f"Title: {article['title']}")
            print(f"Source: {article['source']['name']}")
            print(f"Content: {article['content']}")
            print("-" * 80)
    else:
        print("Error:", trump_news["error"])

    # Example 3: Everything endpoint test
    print("\n=== EVERYTHING ENDPOINT TEST ===\n")

    everything_news = news_service.search_everything(
        query="Nairobi Securities Exchange",
        language="en",
        sort_by="publishedAt",
        page_size=5,
    )

    if everything_news["success"]:
        for article in everything_news["articles"]:
            print(f"Title: {article['title']}")
            print(f"Source: {article['source']['name']}")
            print(f"Published: {article['publishedAt']}")
            print(f"Description: {article['description']}")
            print(f"URL: {article['url']}")
            print(f"Content: {article['content']}")
            print("-" * 80)
    else:
        print("Error:", everything_news["error"])