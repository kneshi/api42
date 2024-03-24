# Async API 42 Scraper with Rate Limiting

This Python script is an asynchronous api scraper that fetches data from 42 while respecting a specified rate limit (based on your app). Hoping this script scrape data without overwhelming 42 servers with too many requests.

## Features

- **Asynchronous Requests**: perform asynchronous HTTP requests, maximizing efficiency and reducing latency.
- **Rate Limiting**: rate limiting mechanism to not exceed a specified number of requests per second
- **Retries and Backoff**: retry logic with exponential backoff in case of rate limit violations or failures.
- **Pages handling**: By default get 100 items per page to reduce the number of requests


## Configuration

The script requires the following configuration in the `api42config.py` file:

```python
import os

TOKEN_URL = "https://api.intra.42.fr/oauth/token"
ENDPOINT = "https://api.intra.42.fr/v2"

RATE_LIMIT = 8  
PER_PAGE = 100

PARAMS = {
    "client_id": "xxxxx",
    "client_secret": "xxxxx",
    "grant_type": "client_credentials",
    "scope": "xxxxx"
}

```