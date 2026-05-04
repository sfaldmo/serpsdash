import os
import urllib.request
import urllib.parse
import json
import sqlite3
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


def fetch_keyword(keyword, week_date_str, db_path, api_key=None):
    key = api_key or os.environ.get('SCALESERP_API_KEY', '')
    if not key:
        raise ValueError('SCALESERP_API_KEY environment variable not set')

    all_results = []
    global_pos = 0
    for page in range(1, PAGES_TO_FETCH + 1):
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
        req = urllib.request.Request(url, headers={'User-Agent': 'SERP-Dashboard/1.0'})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            print(f'[fetch_keyword] page {page} error for "{keyword}": {e}')
            break

        organic = data.get('organic_results', [])
        if not organic:
            break
        for result in organic:
            global_pos += 1
            all_results.append((global_pos, page, result))

    conn = sqlite3.connect(db_path)
    keyword_id = get_or_create_keyword(conn, keyword)
    week_id    = get_or_create_week(conn, week_date_str)

    # Clear existing results so a re-fetch replaces rather than duplicates
    conn.execute(
        'DELETE FROM serp_results WHERE keyword_id=? AND week_id=?',
        (keyword_id, week_id)
    )

    imported = 0
    for position, google_page, result in all_results:
        link    = result.get('link', '').strip()
        title   = result.get('title', '').strip()
        snippet = result.get('snippet', '').strip()
        if link:
            insert_result(conn, keyword_id, week_id, position, link, title, snippet, google_page)
            imported += 1

    conn.commit()
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
