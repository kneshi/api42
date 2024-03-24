import aiohttp
import time
import asyncio
import json
import logging
import re
import time
import logging
from typing import List, Dict, Optional
from api42config import TOKEN_URL, ENDPOINT, PARAMS

logging.basicConfig(level=logging.INFO)

RATE_LIMIT = 8  # Define the rate limit (requests per second)
PER_PAGE = 100


logging.basicConfig(level=logging.INFO)
last_request_time = time.time()
request_count = 0


class APIScraperError(Exception):
    """Base class for API scraper errors."""
    pass

class TokenRetrievalError(APIScraperError):
    """Error raised when failed to retrieve token."""
    pass

class PageFetchError(APIScraperError):
    """Error raised when failed to fetch page."""
    pass

class RateLimiter:
    """
    Class to handle rate limiting for API requests.
    """
    def __init__(self, rate_limit: int):
        self.rate_limit = rate_limit
        self.last_request_time = time.time()
        self.request_count = 0

    async def wait_if_needed(self):
        """Wait if rate limit is reached."""
        elapsed_time = time.time() - self.last_request_time
        if elapsed_time < 1:
            if self.request_count >= self.rate_limit:
                await asyncio.sleep(1 - elapsed_time)
                self.request_count = 0
                self.last_request_time = time.time()
            else:
                self.request_count += 1
        else:
            self.request_count = 1
            self.last_request_time = time.time()

async def get_token(session: aiohttp.ClientSession) -> str:
    """
    Retrieve an access token from the token URL.
    
    Args:
        session: aiohttp ClientSession instance.
        
    Returns:
        Access token string.
        
    Raises:
        TokenRetrievalError: If failed to retrieve token.
    """
    try:
        async with session.post(TOKEN_URL, data=PARAMS) as response:
            response.raise_for_status()
            data = await response.json()
            return data["access_token"]
    except aiohttp.ClientError as e:
        raise TokenRetrievalError(f"Failed to retrieve token: {e}") from e

async def fetch_page(session: aiohttp.ClientSession, url: str, headers: Optional[Dict[str, str]] = None) -> Optional[str]:
    """
    Fetch a page from the given URL.
    
    Args:
        session: aiohttp ClientSession instance.
        url: URL of the page to fetch.
        headers: Optional headers to include in the request.
        
    Returns:
        Page content as a string, or None if failed to fetch.
    """
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.text()
    except aiohttp.ClientError as e:
        logging.error(f"Failed to fetch page: {e}")
        return None

async def get_pmax(session: aiohttp.ClientSession, qr: str, token: str) -> int:
    """
    Get the maximum page number for the given query.
    
    Args:
        session: aiohttp ClientSession instance.
        qr: Query string.
        token: Access token.
        
    Returns:
        Maximum page number.
    """
    try:
        url = f"{ENDPOINT}/{qr}{get_qsep(qr)}per_page={PER_PAGE}"
        async with session.get(url, headers={"Authorization": f"Bearer {token}"}) as response:
            response.raise_for_status()
            link_header = response.headers.get("Link")
            if link_header:
                last_page_match = re.search(r"page=(\d+)(?=[^,]*\brel=\"last\")", link_header)
                if last_page_match:
                    return int(last_page_match.group(1))
    except (aiohttp.ClientError, KeyError, AttributeError) as e:
        logging.error(f"Failed to get page max: {e}")
    return 1

async def throttled_fetch(session: aiohttp.ClientSession, url: str, headers: Optional[Dict[str, str]] = None, retries: int = 3) -> Optional[str]:
    """
    Fetch a page from the given URL while respecting the rate limit.
    
    Args:
        session: aiohttp ClientSession instance.
        url: URL of the page to fetch.
        headers: Optional headers to include in the request.
        retries: Number of retries in case of failure.
        
    Returns:
        Page content as a string, or None if failed to fetch after retries.
    """
    rate_limiter = RateLimiter(RATE_LIMIT)
    for attempt in range(retries):
        try:
            await rate_limiter.wait_if_needed()
            start_time = time.time()
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as e:
            logging.error(f"Failed to fetch page: {e}")
            if attempt == retries - 1:
                raise PageFetchError(f"Failed to fetch page after {retries} retries.") from e
            elif response.status == 429:  # Rate limit exceeded
                retry_after = int(response.headers.get('Retry-After', '5'))  # Default 5 seconds
                logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
                await asyncio.sleep(retry_after)
                print(f"Retrying request for URL: {url}")
            else:
                raise PageFetchError(f"Failed to fetch page: {e}") from e
        finally:
            time_taken = time.time() - start_time
            time_to_wait = max(0, 1 / RATE_LIMIT - time_taken)
            await asyncio.sleep(time_to_wait)

async def scrapper(session: aiohttp.ClientSession, qr: str, token: str) -> List[Dict]:
    """
    Scrape data from the API endpoint for the given query.
    
    Args:
        session: aiohttp ClientSession instance.
        qr: Query string.
        token: Access token.
        
    Returns:
        List of dictionaries containing the scraped data.
    """
    try:
        page_max = await get_pmax(session, qr, token)
        reqs = [f"{ENDPOINT}/{qr}{get_qsep(qr)}page={page}" for page in range(1, page_max + 1)]
        print("Requesting pages..." + str(reqs))
        pages = await asyncio.gather(*[throttled_fetch(session, req, headers={"Authorization": f"Bearer {token}"}) for req in reqs])
        sorted_pages = sorted(pages, key=lambda x: int(re.search(r"page=(\d+)", x).group(1)) if re.search(r"page=(\d+)", x) else 0)

        return [json.loads(page) for page in sorted_pages if page]
    except Exception as e:
        logging.error(f"Error in scrapper: {e}")
        return []

def get_qsep(qr: str) -> str:
    """
    Get the query string separator based on the query string.
    
    Args:
        qr: Query string.
        
    Returns:
        Query string separator ('?' or '&').
    """
    return "?" if qr.find("?") < 0 else "&"

async def api_scrapper(qr: str) -> List[Dict]:
    """
    Scrape data from the API endpoint for the given query.
    
    Args:
        qr: Query string.
        
    Returns:
        List of dictionaries containing the scraped data.
    """
    try:
        async with aiohttp.ClientSession() as session:
            token = await get_token(session)
            if token:
                return await scrapper(session, qr, token)
            else:
                raise TokenRetrievalError("Failed to obtain token.")
    except Exception as e:
        logging.error(f"Error in api_scrapper: {e}")
        return []

def get_data_from_api(url: str) -> List[Dict]:
    """
    Get data from the API for the given URL.
    
    Args:
        url: URL to fetch data from.
        
    Returns:
        List of dictionaries containing the fetched data.
    """
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(api_scrapper(url))
    return data

# Testing the function
if __name__ == "__main__":
    data = get_data_from_api("quests_users?filter[quest_id]=37&filter[validated]=true&filter[campus_id]=31&sort=validated_at")
    # print(data)

