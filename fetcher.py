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
#   Phase 1 - one max_page request for pages 1..FIRST_BLOCK (5 credits), with
#     page 1 vetted against last week. All five pages must come from the same
#     request: each ScaleSERP request is an independent draw, and when pages
#     were fetched one at a time (2026-10-12) a vetted page 1 got paired with
#     off-topic pages 2-5 - 3-5x the usual share of "new" deep results. A bad
#     draw is redrawn whole, up to SANITY_ATTEMPTS times.
#   Phase 2 - if we still have fewer than TARGET_MIN results, pull single pages
#     after the deepest one we got, up to MAX_PAGES.
FIRST_BLOCK     = 5     # pages fetched in the single max_page request (API cap)
MAX_PAGES       = 10    # never page deeper than this
TARGET_MIN      = 30    # stop early once a keyword reaches this many results
DEEP_EMPTY_STOP = 2     # give up deep paging after this many consecutive empties

REQUEST_TIMEOUT = 60    # one multi-page request takes longer than a single page
REQUEST_SPACING = 0.6   # seconds between phase-2 single-page requests
MAX_ATTEMPTS    = 6     # initial try + 5 retries (ScaleSERP HF-flaps in bursts)
BACKOFF_CAP     = 8     # seconds; don't let exponential backoff run away
RETRY_STATUSES  = {408, 429, 500, 502, 503, 504}

# ScaleSERP reports failures two ways. Most are transient server-side hiccups
# (e.g. code HF: "unable to fulfil your request at this time, please try again
# later ... you have not been charged") and must be retried, not fatal. Only a
# few - out of credits, bad API key, a malformed query - will never succeed on
# retry, so we fail fast ONLY when the message matches one of these markers and
# retry everything else.
# Sanity check against last week. ScaleSERP occasionally hands back deep or
# off-topic pages labelled as page 1 (e.g. 2026-09-28: "The Wellness Company"
# page 1 was UK company filings and Chinese app-store listings, and Melaleuca
# Reviews' "page 1" was URLs that had ranked #19-#62). Real week-over-week
# movement never looks like that: across Jul-Sep 2026 at least 4 of every
# keyword's top 10 were in the previous week's top 30, while the bad fetches
# scored 0. So if fewer than SANITY_MIN_OVERLAP carry over, refetch, and if it
# is still off, refuse to save rather than silently storing junk.
SANITY_TOP          = 10    # fresh results compared
SANITY_PREV_DEPTH   = 30    # ...against this many of last week's results
SANITY_MIN_OVERLAP  = 3
SANITY_ATTEMPTS     = 3     # full refetches before giving up (5 credits each)
SANITY_ANCHOR_TOP   = 5     # last week's #1 URL must be in the new top 5
# 2026-10-06: back-to-back identical ScaleSERP requests for "Riverbend Ranch"
# flipped between the real SERP and an off-topic one (TN Facebook videos,
# MapQuest WA, Reddit) about half the time, even with a fixed location; real
# Google showed the normal SERP. The off-topic variant can still share 3 URLs
# with last week's top 30, so also require last week's exact #1 URL near the
# top. Matching on domain wasn't enough: 2026-10-12's off-topic "Melaleuca"
# SERP had melaleuca.com/offline at #5 but not the homepage. Across Jul-Oct
# 2026 the exact-URL rule held for 90 of 94 week pairs; the misses were real #1
# changes (Melaleuca Reviews' Reddit/melaleuca.com flip, Riverbend 9/14).

PERMANENT_ERROR_MARKERS = (
    'credit', 'not enough', 'api key', 'api_key', 'invalid api',
    'unauthorized', 'forbidden', 'subscription', 'suspend', 'disabled',
)


def _is_permanent_error(msg):
    m = (msg or '').lower()
    return any(marker in m for marker in PERMANENT_ERROR_MARKERS)


class FetchError(Exception):
    """Raised when a keyword could not be fetched after retries."""


class SuspiciousFetchError(FetchError):
    """Raised when fetched results look nothing like the previous week's."""


def _fetch_serp(keyword, key, max_page=None, page=None, attempts=None):
    """Fetch SERP organic_results from ScaleSERP.

    Pass `max_page` to pull pages 1..max_page in one request (each result then
    carries a true `page` and `position_overall`); or `page` to pull a single
    page. Retries transient failures (timeouts, 429, 5xx) with exponential
    backoff, up to `attempts` tries (default MAX_ATTEMPTS). Raises FetchError if
    it still cannot be fetched, so callers can tell 'no more results' apart from
    'the request failed'.
    """
    attempts = attempts or MAX_ATTEMPTS
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

    for attempt in range(1, attempts + 1):
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
            if e.code == 402:
                raise FetchError('ScaleSERP is out of credits (HTTP 402). Fetches will work '
                                 'again after the monthly credit reset.') from e
            if e.code not in RETRY_STATUSES:
                raise FetchError(f'HTTP {e.code}') from e
        except Exception as e:
            last_err = str(e) or e.__class__.__name__

        if attempt < attempts:
            # Exponential backoff with jitter (capped) to avoid hammering a
            # throttled or flapping API without waiting minutes.
            time.sleep(min(2 ** (attempt - 1), BACKOFF_CAP) + random.uniform(0, 0.4))

    raise FetchError(f'fetch failed after {attempts} attempts: {last_err}')


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


def _previous_urls(db_path, keyword, week_date_str):
    """Normalized top-SANITY_PREV_DEPTH URLs from the most recent earlier week
    that has results for this keyword, or None if there is no such week."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute('''
            SELECT sr.url FROM serp_results sr
            JOIN   weeks w    ON sr.week_id = w.id
            JOIN   keywords k ON sr.keyword_id = k.id
            WHERE  k.name = ? AND w.week_date = (
                SELECT MAX(w2.week_date) FROM serp_results sr2
                JOIN   weeks w2 ON sr2.week_id = w2.id
                WHERE  sr2.keyword_id = k.id AND w2.week_date < ?)
            ORDER  BY sr.position LIMIT ?
        ''', (keyword, week_date_str, SANITY_PREV_DEPTH)).fetchall()
    finally:
        conn.close()
    return [normalize_url(r[0]) for r in rows] or None


def _anchor_held(all_results, prev_urls):
    """True if last week's #1 URL is in the fresh top SANITY_ANCHOR_TOP."""
    top = {normalize_url((res.get('link') or '').strip()) for _, _, res in all_results[:SANITY_ANCHOR_TOP]}
    return prev_urls[0] in top


def _overlap(all_results, prev_urls):
    top = [normalize_url((res.get('link') or '').strip()) for _, _, res in all_results[:SANITY_TOP]]
    prev = set(prev_urls)
    return sum(u in prev for u in top)


def _collect_keyword(keyword, key, check=None):
    """Pull up to MAX_PAGES of organic results for one keyword. Returns a list
    of (position, google_page, result), densely ranked from 1.

    `check(results)` vets page 1 of each pages-1..FIRST_BLOCK draw: it returns
    None if it looks right, else a reason. A bad draw is redrawn whole up to
    SANITY_ATTEMPTS times, then SuspiciousFetchError is raised."""
    for attempt in range(1, SANITY_ATTEMPTS + 1):
        all_results = []
        seen_urls = set()
        try:
            organic = _fetch_serp(keyword, key, max_page=FIRST_BLOCK)
        except FetchError:
            organic = []
        # Results without a real `page` can't be placed, so treat the draw as
        # failed and fall back to page 1 alone (deeper pages come in phase 2).
        if organic and all(r.get('page') for r in organic if r.get('link')):
            _collect(organic, all_results, seen_urls, fallback_page=1)
        else:
            _collect(_fetch_serp(keyword, key, page=1), all_results, seen_urls, fallback_page=1)
        page1 = [t for t in all_results if t[1] == 1]
        problem = check(page1) if check else None
        if not problem:
            break
        if attempt == SANITY_ATTEMPTS:
            raise SuspiciousFetchError(problem)
        time.sleep(REQUEST_SPACING)
    deepest = max((gp for _, gp, _ in all_results), default=1)

    # Phase 2: keep pulling single pages until we hit TARGET_MIN or run dry.
    consecutive_empty = 0
    page = deepest + 1   # resume right after the deepest page phase 1 returned
    while len(all_results) < TARGET_MIN and page <= MAX_PAGES and consecutive_empty < DEEP_EMPTY_STOP:
        time.sleep(REQUEST_SPACING)
        try:
            organic = _fetch_serp(keyword, key, page=page)
        except FetchError:
            # A page failed even after retries (e.g. a ScaleSERP HF outage). If we
            # already have results, keep them - a partial fetch beats losing the
            # keyword, and a later re-fetch tops it up. If we have nothing at all,
            # surface the real error (and leave existing data untouched below).
            if all_results:
                break
            raise
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
    return [(i + 1, gp, res) for i, (_, gp, res) in enumerate(all_results)]


def fetch_keyword(keyword, week_date_str, db_path, api_key=None, force=False):
    """Fetch and store one keyword. Unless `force`, results that fail the
    week-over-week sanity check are refetched and, failing that, rejected with
    SuspiciousFetchError (existing data left untouched)."""
    key = api_key or os.environ.get('SCALESERP_API_KEY', '')
    if not key:
        raise ValueError('SCALESERP_API_KEY environment variable not set')

    prev_urls = None if force else _previous_urls(db_path, keyword, week_date_str)

    def check(results):
        if prev_urls is None:
            return None
        overlap = _overlap(results, prev_urls)
        if overlap < SANITY_MIN_OVERLAP:
            why = (f"only {overlap} of the top {SANITY_TOP} were in last week's top "
                   f"{SANITY_PREV_DEPTH} (normally 4+)")
        elif not _anchor_held(results, prev_urls):
            why = f"last week's #1 ({prev_urls[0]}) is not in the top {SANITY_ANCHOR_TOP}"
        else:
            return None
        return (f'"{keyword}" results look wrong after {SANITY_ATTEMPTS} tries: {why}. ScaleSERP '
                f'likely returned an off-topic SERP. Existing data left untouched; refetch later, '
                f'or force-save if the SERP really did change.')

    all_results = _collect_keyword(keyword, key, check)

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
