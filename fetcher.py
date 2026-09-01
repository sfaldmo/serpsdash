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
# returns ~5-10 organic results (SERP features eat organic slots, so a page is
# never a clean 10) and pagination is mandatory.
#
# We fetch in two phases:
#   Phase 1 - one `max_page` request for pages 1..FIRST_BLOCK. ScaleSERP pages
#     server-side and tags every result with its true `page` and
#     `position_overall`, so we get authoritative page boundaries in one call.
#     max_page is capped at 5 for real-time searches.
#   Phase 2 - if we still have fewer than TARGET_MIN results, keep pulling one
#     page at a time (page 6, 7, ...) up to MAX_PAGES, labelling each with its
#     request page, until we hit the target or results dry up. This also
#     transparently recovers if a max_page request "collapses" to a single page
#     (a rare ScaleSERP glitch) - the deep loop just refetches the missing pages.
FIRST_BLOCK     = 5     # pages fetched in the single max_page request (API cap)
MAX_PAGES       = 10    # never page deeper than this
TARGET_MIN      = 30    # stop early once a keyword reaches this many results
DEEP_EMPTY_STOP = 2     # give up deep paging after this many consecutive empties

REQUEST_TIMEOUT = 60    # one multi-page request takes longer than a single page
REQUEST_SPACING = 0.6   # seconds between phase-2 single-page requests
MAX_ATTEMPTS    = 4     # initial try + 3 retries
RETRY_STATUSES  = {408, 429, 500, 502, 503, 504}

# ScaleSERP reports failures two ways. Most are transient server-side hiccups
# (e.g. code HF: "unable to fulfil your request at this time, please try again
# later ... you have not been charged") and must be retried, not fatal. Only a
# few - out of credits, bad API key, a malformed query - will never succeed on
# retry, so we fail fast ONLY when the message matches one of these markers and
# retry everything else.
PERMANENT_ERROR_MARKERS = (
    'credit', 'not enough', 'api key', 'api_key', 'invalid api',
    'unauthorized', 'forbidden', 'subscription', 'suspend', 'disabled',
)


def _is_permanent_error(msg):
    m = (msg or '').lower()
    return any(marker in m for marker in PERMANENT_ERROR_MARKERS)


class FetchError(Exception):
    """Raised when a keyword could not be fetched after retries."""


def _fetch_serp(keyword, key, max_page=None, page=None):
    """Fetch SERP organic_results from ScaleSERP.

    Pass `max_page` to pull pages 1..max_page in one request (each result then
    carries a true `page` and `position_overall`); or `page` to pull a single
    page. Retries transient failures (timeouts, 429, 5xx) with exponential
    backoff. Raises FetchError if it still cannot be fetched, so callers can
    tell 'no more results' apart from 'the request failed'.
    """
    params = {
        'api_key': key,
        'q': keyword,
        'num': 10,                 # per-page cap; Google returns <=10 anyway
        'output': 'json',
        'google_domain': 'google.com',
        'gl': 'us',
        'hl': 'en',
        'device': 'desktop',
    }
    if max_page is not None:
        params['max_page'] = max_page
    if page is not None:
        params['page'] = page
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
                if _is_permanent_error(msg):
                    # Credits/auth/bad-query: retrying cannot help - fail fast.
                    raise FetchError(f'ScaleSERP rejected the request: {msg}')
                # Transient server-side failure (e.g. HF). Fall through to the
                # backoff below and retry like any other transient error.
                last_err = f'ScaleSERP: {msg}'
            else:
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


def _collect(organic, kept, seen_urls, fallback_page):
    """Add unique organic results from one response into `kept`.

    Each entry is (position, google_page, result). Trusts ScaleSERP's own
    `position`/`page` when present (max_page responses); otherwise assigns a
    dense position and uses `fallback_page` (single-page responses have no
    page field). Returns how many new results were added.
    """
    added = 0
    for result in organic:
        link = (result.get('link') or '').strip()
        if not link:
            continue
        # Pages can overlap; keep the first (highest-ranked) occurrence of a URL.
        norm = normalize_url(link)
        if norm in seen_urls:
            continue
        seen_urls.add(norm)

        google_page = result.get('page') or fallback_page
        # position is renumbered densely at the end, so a placeholder is fine;
        # keep ScaleSERP's position_overall when present to preserve ordering.
        position = result.get('position_overall')
        kept.append((position, google_page, result))
        added += 1
    return added


def fetch_keyword(keyword, week_date_str, db_path, api_key=None):
    key = api_key or os.environ.get('SCALESERP_API_KEY', '')
    if not key:
        raise ValueError('SCALESERP_API_KEY environment variable not set')

    all_results = []
    seen_urls = set()

    # Phase 1: pages 1..FIRST_BLOCK in one authoritative max_page request.
    organic = _fetch_serp(keyword, key, max_page=FIRST_BLOCK)
    _collect(organic, all_results, seen_urls, fallback_page=1)
    # Deepest real page we actually got back (guards against a max_page request
    # collapsing to a single page - the deep loop resumes from the next page).
    deepest = max((gp for _, gp, _ in all_results), default=0)

    # Phase 2: keep pulling single pages until we hit TARGET_MIN or run dry.
    consecutive_empty = 0
    page = deepest + 1   # resume right after the deepest page phase 1 returned
    while len(all_results) < TARGET_MIN and page <= MAX_PAGES and consecutive_empty < DEEP_EMPTY_STOP:
        time.sleep(REQUEST_SPACING)
        organic = _fetch_serp(keyword, key, page=page)
        if _collect(organic, all_results, seen_urls, fallback_page=page) == 0:
            consecutive_empty += 1
        else:
            consecutive_empty = 0
        page += 1

    # Never destroy the existing week's data on an empty fetch - a wiped row
    # set is indistinguishable from a keyword that legitimately lost rankings.
    if not all_results:
        raise FetchError(f'no organic results returned for "{keyword}" - existing data left untouched')

    # Order by (page, position) so rows read top-to-bottom, then renumber
    # densely: position becomes the overall rank across everything we kept.
    all_results.sort(key=lambda t: (t[1], t[0] if t[0] is not None else 1e9))
    all_results = [(i + 1, gp, res) for i, (_, gp, res) in enumerate(all_results)]

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
