import os
import time
import random
import urllib.request
import urllib.error
import urllib.parse
import json
import sqlite3
from importer import get_or_create_keyword, get_or_create_week, insert_result, normalize_url

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


# Google removed the num=100 parameter in Sept 2025, so every SERP page now
# returns ~10 organic results and pagination is mandatory. ScaleSERP's
# `max_page` fetches several pages in ONE request and tags every result with
# its true `page` and `position_overall` — the real Google page boundaries,
# which are variable (SERP features eat organic slots, so a page holds 5-10,
# never a clean 10). We store those authoritative values instead of guessing.
#
# max_page is capped at 5 for real-time searches. We only need 3.
PAGES_TO_FETCH  = 3
REQUEST_TIMEOUT = 60    # one multi-page request takes longer than a single page
MAX_ATTEMPTS    = 4     # initial try + 3 retries
RETRY_STATUSES  = {408, 429, 500, 502, 503, 504}


class FetchError(Exception):
    """Raised when a keyword could not be fetched after retries."""


def _fetch_serp(keyword, key, max_page):
    """Fetch `max_page` Google pages in one request. Returns organic_results.

    Each result carries `page` and `position_overall` assigned by ScaleSERP.
    Retries transient failures (timeouts, 429, 5xx) with exponential backoff.
    Raises FetchError if it still cannot be fetched, so callers can tell
    'no more results' apart from 'the request failed'.
    """
    params = {
        'api_key': key,
        'q': keyword,
        'max_page': max_page,
        'num': 10,                 # per-page cap; Google returns <=10 anyway
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
            req = urllib.request.Request(url, headers={'User-Agent': 'SERP-Dashboard/2.0'})
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
                raise FetchError(f'HTTP {e.code}') from e
        except Exception as e:
            last_err = str(e) or e.__class__.__name__

        if attempt < MAX_ATTEMPTS:
            # Exponential backoff with jitter to avoid hammering a throttled API.
            time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.4))

    raise FetchError(f'fetch failed after {MAX_ATTEMPTS} attempts: {last_err}')


def fetch_keyword(keyword, week_date_str, db_path, api_key=None):
    key = api_key or os.environ.get('SCALESERP_API_KEY', '')
    if not key:
        raise ValueError('SCALESERP_API_KEY environment variable not set')

    organic = _fetch_serp(keyword, key, PAGES_TO_FETCH)

    all_results = []
    seen_urls = set()

    for result in organic:
        link = (result.get('link') or '').strip()
        if not link:
            continue
        # Pages can overlap; keep the first (highest-ranked) occurrence of a URL.
        norm = normalize_url(link)
        if norm in seen_urls:
            continue
        seen_urls.add(norm)

        # Trust ScaleSERP's own numbering. Fall back defensively only if a
        # result is missing it (older responses / edge cases).
        position    = result.get('position_overall')
        google_page = result.get('page')
        if position is None:
            position = len(all_results) + 1
        if google_page is None:
            google_page = -(-position // 10)   # ceil(position / 10)

        all_results.append((position, google_page, result))

    # Never destroy the existing week's data on an empty fetch - a wiped row
    # set is indistinguishable from a keyword that legitimately lost rankings.
    if not all_results:
        raise FetchError(f'no organic results returned for "{keyword}" - existing data left untouched')

    # Order by true absolute position so DB rows are stored top-to-bottom.
    all_results.sort(key=lambda t: t[0])

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
