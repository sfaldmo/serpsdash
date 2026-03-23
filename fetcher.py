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


def fetch_keyword(keyword, week_date_str, db_path, api_key=None):
    key = api_key or os.environ.get('SCALESERP_API_KEY', '')
    if not key:
        raise ValueError('SCALESERP_API_KEY environment variable not set')

    params = {
        'api_key': key,
        'q': keyword,
        'num': 40,
        'output': 'json',
        'google_domain': 'google.com',
        'gl': 'us',
        'hl': 'en',
    }
    url = f'{SCALESERP_ENDPOINT}?{urllib.parse.urlencode(params)}'

    req = urllib.request.Request(url, headers={'User-Agent': 'SERP-Dashboard/1.0'})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode('utf-8'))

    organic = data.get('organic_results', [])

    conn = sqlite3.connect(db_path)
    keyword_id = get_or_create_keyword(conn, keyword)
    week_id    = get_or_create_week(conn, week_date_str)

    # Clear existing results so a re-fetch replaces rather than duplicates
    conn.execute(
        'DELETE FROM serp_results WHERE keyword_id=? AND week_id=?',
        (keyword_id, week_id)
    )

    imported = 0
    for i, result in enumerate(organic, start=1):
        link    = result.get('link', '').strip()
        title   = result.get('title', '').strip()
        snippet = result.get('snippet', '').strip()
        if link:
            insert_result(conn, keyword_id, week_id, i, link, title, snippet)
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
