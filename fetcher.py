import os
import time
import random
import urllib.request
import urllib.error
import urllib.parse
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from importer import get_or_create_keyword, get_or_create_week, insert_result

SCALESERP_ENDPOINT = 'https://api.scaleserp.com/search'

KEYWORDS = [
    'Melaleuca',
    'Melaleuca.com',
    'Frank VanderSloot',
    'Melaleuca Products',
    'Melaleuca Reviews',
    'The Wellness Company',
    'Riverbend Ranch',
]


PAGES_TO_FETCH = 5  # 5 pages × 10 = up to 50 organic results per keyword

REQUEST_TIMEOUT = 30    # seconds per HTTP request
MAX_ATTEMPTS    = 4     # initial try + 3 retries
MAX_PARALLEL    = 3     # concurrent page requests per keyword
RETRY_STATUSES  = {408, 429, 500, 502, 503, 504}


class FetchError(Exception):
    """Raised when a keyword could not be fetched after retries."""


def _get_page(keyword, page, key):
    """Fetch one SERP page. Returns the organic_results list.

    Retries transient failures (timeouts, 429, 5xx) with exponential backoff.
    Raises FetchError if the page still cannot be fetched, so callers can tell
    'no more results' apart from 'the request failed'.
    """
    params = {
        'api_key': key,
        'q': keyword,
        'page': page,
        'num': 10,
        'output': 'json',
        'google_domain': 'google.com',
        'gl': 'us',
        'hl': 'en',
        'device': 'desktop',
    }
    url = f'{SCALESERP_ENDPOINT}?{urllib.parse.urlencode(params)}'
    last_err = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'SERP-Dashboard/1.0'})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            # ScaleSERP reports some failures as HTTP 200 with success=false.
            info = data.get('request_info') or {}
            if info.get('success') is False:
                msg = info.get('message', 'unknown ScaleSERP error')
                # Credit/auth problems will never succeed on retry - fail fast.
                raise FetchError(f'ScaleSERP rejected the request: {msg}')

            return data.get('organic_results', []) or []

        except FetchError:
            raise
        except urllib.error.HTTPError as e:
            last_err = f'HTTP {e.code}'
            if e.code not in RETRY_STATUSES:
                raise FetchError(f'page {page}: HTTP {e.code}') from e
        except Exception as e:
            last_err = str(e) or e.__class__.__name__

        if attempt < MAX_ATTEMPTS:
            # Exponential backoff with jitter so parallel pages don't sync up.
            time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.4))

    raise FetchError(f'page {page} failed after {MAX_ATTEMPTS} attempts: {last_err}')


def fetch_keyword(keyword, week_date_str, db_path, api_key=None):
    key = api_key or os.environ.get('SCALESERP_API_KEY', '')
    if not key:
        raise ValueError('SCALESERP_API_KEY environment variable not set')

    pages = list(range(1, PAGES_TO_FETCH + 1))

    # Pages are independent, so fetch them concurrently to stay well inside
    # the gunicorn request timeout, then reassemble in page order.
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        organic_by_page = list(pool.map(lambda p: _get_page(keyword, p, key), pages))

    all_results = []
    global_pos = 0
    for page, organic in zip(pages, organic_by_page):
        if not organic:
            break  # genuine end of results
        for result in organic:
            global_pos += 1
            all_results.append((global_pos, page, result))

    # Never destroy the existing week's data on an empty fetch - a wiped row
    # set is indistinguishable from a keyword that legitimately lost rankings.
    if not all_results:
        raise FetchError(f'no organic results returned for "{keyword}" - existing data left untouched')

    conn = sqlite3.connect(db_path)
    try:
        keyword_id = get_or_create_keyword(conn, keyword)
        week_id    = get_or_create_week(conn, week_date_str)

        # Replace rather than duplicate, atomically: if the insert half fails
        # the delete rolls back with it.
        with conn:
            conn.execute(
                'DELETE FROM serp_results WHERE keyword_id=? AND week_id=?',
                (keyword_id, week_id)
            )
            imported = 0
            for position, google_page, result in all_results:
                link    = (result.get('link') or '').strip()
                title   = (result.get('title') or '').strip()
                snippet = (result.get('snippet') or '').strip()
                if link:
                    insert_result(conn, keyword_id, week_id, position, link, title, snippet, google_page)
                    imported += 1
    finally:
        conn.close()

    return imported


def fetch_all(week_date_str, db_path, api_key=None):
    """Fetch all tracked keywords. Returns dict of keyword -> {count, error}."""
    results = {}
    for kw in KEYWORDS:
        try:
            count = fetch_keyword(kw, week_date_str, db_path, api_key)
            results[kw] = {'count': count, 'error': None}
        except Exception as e:
            results[kw] = {'count': 0, 'error': str(e)}
    return results
