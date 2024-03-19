import aiohttp
import time
import asyncio
import json
import logging
import re
import time
from api42config import TOKEN_URL, ENDPOINT, PARAMS

RATE_LIMIT = 8  # Define the rate limit (requests per second)


logging.basicConfig(level=logging.INFO)
last_request_time = time.time()
request_count = 0


async def get_token(session):
    try:
        async with session.post(TOKEN_URL, data=PARAMS) as response:
            response.raise_for_status()
            data = await response.json()
            return data["access_token"]
    except aiohttp.ClientError as e:
        logging.error(f"Failed to retrieve token: {e}")
        return None


async def fetch_page(session, url, headers=None):
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.text()
    except aiohttp.ClientError as e:
        logging.error(f"Failed to fetch page: {e}")
        return None


async def get_pmax(session, qr, token):
    try:
        url = f"{ENDPOINT}/{qr}"
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


async def throttled_fetch(session, url, headers=None, retries=3):
    global last_request_time
    global request_count
    
    # Ensure that requests are made within the rate limit
    elapsed_time = time.time() - last_request_time
    if elapsed_time < 1:
        # If the rate limit is reached, wait until the next second
        if request_count >= RATE_LIMIT:
            await asyncio.sleep(1 - elapsed_time)
            request_count = 0
            last_request_time = time.time()
        else:
            # If within the rate limit, increment the request count
            request_count += 1
    else:
        # If the current second has passed, reset the request count and update the last request time
        request_count = 1
        last_request_time = time.time()

    for attempt in range(retries):
        try:
            start_time = time.time()  # Record the start time of the request
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as e:
            logging.error(f"Failed to fetch page: {e}")
            if attempt == retries - 1:
                return None  # Failed after retries
            elif response.status == 429:  # Rate limit exceeded
                retry_after = int(response.headers.get('Retry-After', '5'))  # Default 5 seconds
                logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
                await asyncio.sleep(retry_after)
                print(f"Retrying request for URL: {url}")
            else:
                return None  # Other client errors
        finally:
            # Calculate the time taken for the request
            time_taken = time.time() - start_time
            # Calculate the time to wait before making the next request
            time_to_wait = max(0, 1 / RATE_LIMIT - time_taken)
            # Wait before making the next request
            await asyncio.sleep(time_to_wait)



async def scrapper(session, qr, token):
    try:
        page_max = await get_pmax(session, qr, token)
        reqs = [f"{ENDPOINT}/{qr}{get_qsep(qr)}page={page}" for page in range(1, page_max + 1)]
        pages = []
        pages = await asyncio.gather(*[throttled_fetch(session, req, headers={"Authorization": f"Bearer {token}"}) for req in reqs])
        sorted_pages = sorted(pages, key=lambda x: int(re.search(r"page=(\d+)", x).group(1)) if re.search(r"page=(\d+)", x) else 0)

        return [json.loads(page) for page in sorted_pages if page]
    except Exception as e:
        logging.error(f"Error in scrapper: {e}")
        return []


async def api_scrapper(qr):
    try:
        async with aiohttp.ClientSession() as session:
            token = await get_token(session)
            if token:
                return await scrapper(session, qr, token)
            else:
                logging.error("Failed to obtain token.")
                return []
    except Exception as e:
        logging.error(f"Error in api_scrapper: {e}")
        return []

def get_qsep(qr):
    return "?" if qr.find("?") < 0 else "&"


# Testing the function
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(api_scrapper("cursus_users?filter[campus_id]=31&filter[active]=true"))
    data = json.dumps(data, indent=4)
    print(data)
