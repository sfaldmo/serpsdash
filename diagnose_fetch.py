#!/usr/bin/env python3
"""Diagnose ScaleSERP pagination. Read-only: touches no database.

Run with the key from Railway:
    SCALESERP_API_KEY=xxx /usr/local/bin/python3 diagnose_fetch.py

Google removed num=100 in Sept 2025, so every page now returns ~10 organic
results and pagination is mandatory. This script answers:
  1. Does max_page (server-side multi-page) return per-result `page` and
     `position_overall` fields we can trust? (fixes wrong-page bug)
  2. How many organic results does each real Google page hold? (variable,
     because SERP features eat slots)
  3. Does one max_page request match looping page=1..N serially, for count?
"""
import os
import sys
import json
import time
import urllib.parse
import urllib.request

KEY = os.environ.get('SCALESERP_API_KEY', '')
if not KEY:
    sys.exit('SCALESERP_API_KEY not set. Grab it from Railway > Variables.')

ENDPOINT = 'https://api.scaleserp.com/search'
KEYWORDS = ['Melaleuca', 'Melaleuca Products', 'The Wellness Company']
DEPTH    = 3   # pages the dashboard actually needs


def call(params):
    url = f'{ENDPOINT}?{urllib.parse.urlencode(params)}'
    req = urllib.request.Request(url, headers={'User-Agent': 'SERP-Dashboard-Diag/2.0'})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode('utf-8'))


BASE = {
    'api_key': KEY, 'output': 'json', 'google_domain': 'google.com',
    'gl': 'us', 'hl': 'en', 'device': 'desktop',
}


def show_fields(organic, limit=3):
    """Print which of page / position / position_overall each result carries."""
    for r in organic[:limit]:
        print('       fields:', {
            'position':         r.get('position'),
            'page':             r.get('page'),
            'position_overall': r.get('position_overall'),
            'link':             (r.get('link') or '')[:60],
        })


for kw in KEYWORDS:
    print(f'\n{"="*72}\nKEYWORD: {kw}\n{"="*72}')

    # --- Approach A: what the dashboard does now (loop page=1..DEPTH, num=10)
    print(f'\n-- CURRENT: serial loop page=1..{DEPTH}, num=10 --')
    serial_total = 0
    serial_pages = {}
    for p in range(1, DEPTH + 1):
        try:
            data = call({**BASE, 'q': kw, 'page': p, 'num': 10})
            organic = data.get('organic_results') or []
            serial_pages[p] = len(organic)
            serial_total += len(organic)
            print(f'   page {p}: {len(organic)} organic')
            if p == 1:
                show_fields(organic)
        except Exception as e:
            print(f'   page {p}: ERROR {type(e).__name__}: {e}')
        time.sleep(1.0)

    # --- Approach B: proposed fix (one request, max_page=DEPTH)
    print(f'\n-- PROPOSED: single request, max_page={DEPTH} --')
    try:
        data = call({**BASE, 'q': kw, 'max_page': DEPTH, 'num': 10})
        organic = data.get('organic_results') or []
        print(f'   total organic in one call: {len(organic)}')
        # Count per real Google page as reported BY THE API
        by_page = {}
        for r in organic:
            by_page[r.get('page')] = by_page.get(r.get('page'), 0) + 1
        print(f'   organic per API-reported page: {dict(sorted(by_page.items(), key=lambda x:(x[0] is None, x[0])))}')
        has_page = all(r.get('page') is not None for r in organic)
        has_over = all(r.get('position_overall') is not None for r in organic)
        print(f'   every result has `page`?            {has_page}')
        print(f'   every result has `position_overall`? {has_over}')
        show_fields(organic, limit=5)
    except Exception as e:
        print(f'   ERROR {type(e).__name__}: {e}')
        organic = []

    b_total = len(organic)
    verdict = 'SAME' if serial_total == b_total else f'DIFF (serial={serial_total} vs max_page={b_total})'
    print(f'\n   COUNT: serial={serial_total}  max_page={b_total}   {verdict}')

print('\nDone. Paste this whole output back.')
