from flask import Flask, render_template, request, jsonify, redirect, url_for, Response
import sqlite3
import os
import json
import functools
from datetime import datetime

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Basic Auth
# ---------------------------------------------------------------------------

def check_auth(password):
    expected = os.environ.get('DASHBOARD_PASSWORD', '')
    return expected and password == expected

def require_auth():
    return Response(
        'Access denied', 401,
        {'WWW-Authenticate': 'Basic realm="SERP Dashboard"'}
    )

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not os.environ.get('DASHBOARD_PASSWORD'):
            return f(*args, **kwargs)  # no password set — open access
        auth = request.authorization
        if not auth or not check_auth(auth.password):
            return require_auth()
        return f(*args, **kwargs)
    return decorated

app.before_request_funcs.setdefault(None, [])

@app.before_request
def protect():
    if not os.environ.get('DASHBOARD_PASSWORD'):
        return  # no password configured, allow all
    auth = request.authorization
    if not auth or not check_auth(auth.password):
        return require_auth()
DB_PATH = os.environ.get(
    'DATABASE_PATH',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'serp_dashboard.db')
)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS keywords (
            id   INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            slug TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS weeks (
            id        INTEGER PRIMARY KEY,
            week_date DATE UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS serp_results (
            id         INTEGER PRIMARY KEY,
            keyword_id INTEGER NOT NULL REFERENCES keywords(id),
            week_id    INTEGER NOT NULL REFERENCES weeks(id),
            position   INTEGER NOT NULL,
            url        TEXT NOT NULL,
            title      TEXT,
            snippet    TEXT,
            UNIQUE(keyword_id, week_id, position)
        );

        CREATE TABLE IF NOT EXISTS url_tags (
            id        INTEGER PRIMARY KEY,
            url       TEXT UNIQUE NOT NULL,
            sentiment TEXT NOT NULL DEFAULT "neutral"
                          CHECK(sentiment IN ("positive","negative","neutral")),
            notes     TEXT DEFAULT "",
            owned     INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_sr_kw_week ON serp_results(keyword_id, week_id);
        CREATE INDEX IF NOT EXISTS idx_sr_url     ON serp_results(url);
    ''')
    conn.commit()

    # Migration: add owned column to existing databases
    try:
        conn.execute('ALTER TABLE url_tags ADD COLUMN owned INTEGER NOT NULL DEFAULT 0')
        conn.commit()
    except Exception:
        pass  # column already exists

    conn.close()


# Ensure the directory for the DB file exists (needed when using a Railway volume path like /data)
_db_dir = os.path.dirname(DB_PATH)
if _db_dir:
    try:
        os.makedirs(_db_dir, exist_ok=True)
    except OSError:
        pass  # directory may already exist or be read-only; sqlite will surface the real error

# Run at import time so gunicorn workers always have a valid schema
init_db()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/robots.txt')
def robots():
    return Response('User-agent: *\nDisallow: /\n', mimetype='text/plain')


@app.route('/')
def index():
    conn = get_db()
    keywords = conn.execute('''
        SELECT * FROM keywords ORDER BY CASE name
            WHEN 'Melaleuca' THEN 1
            WHEN 'Melaleuca.com' THEN 2
            WHEN 'Frank VanderSloot' THEN 3
            WHEN 'Melaleuca Products' THEN 4
            WHEN 'Melaleuca Reviews' THEN 5
            WHEN 'The Wellness Company' THEN 6
            WHEN 'Riverbend Ranch' THEN 7
            ELSE 8
        END
    ''').fetchall()
    weeks    = conn.execute('SELECT * FROM weeks ORDER BY week_date DESC').fetchall()
    conn.close()
    return render_template('index.html', keywords=keywords, weeks=weeks)


@app.route('/api/results')
def api_results():
    keyword_id = request.args.get('keyword_id', type=int)
    week_id    = request.args.get('week_id',    type=int)

    if not keyword_id or not week_id:
        return jsonify([])

    conn = get_db()

    # Current week
    rows = conn.execute('''
        SELECT sr.position, sr.url, sr.title, sr.snippet, ut.sentiment, ut.owned
        FROM   serp_results sr
        LEFT JOIN url_tags ut ON sr.url = ut.url
        WHERE  sr.keyword_id = ? AND sr.week_id = ?
        ORDER  BY sr.position
        LIMIT 100
    ''', (keyword_id, week_id)).fetchall()

    # Previous week
    prev = conn.execute('''
        SELECT id FROM weeks
        WHERE  week_date < (SELECT week_date FROM weeks WHERE id = ?)
        ORDER  BY week_date DESC LIMIT 1
    ''', (week_id,)).fetchone()

    prev_pos = {}
    if prev:
        for r in conn.execute('''
            SELECT url, position FROM serp_results
            WHERE keyword_id = ? AND week_id = ?
        ''', (keyword_id, prev['id'])).fetchall():
            prev_pos[r['url']] = r['position']

    # For URLs absent from the previous week, look up their most recent
    # historical position across ALL prior weeks (not just the last one).
    curr_week_date = conn.execute(
        'SELECT week_date FROM weeks WHERE id=?', (week_id,)
    ).fetchone()['week_date']

    not_in_prev = list({r['url'] for r in rows if r['url'] not in prev_pos})
    last_seen = {}
    if not_in_prev:
        placeholders = ','.join(['?'] * len(not_in_prev))
        for hr in conn.execute(f'''
            SELECT url, position, week_date FROM (
                SELECT sr.url, sr.position, w.week_date,
                       ROW_NUMBER() OVER (PARTITION BY sr.url ORDER BY w.week_date DESC) AS rn
                FROM   serp_results sr
                JOIN   weeks w ON sr.week_id = w.id
                WHERE  sr.keyword_id = ? AND sr.url IN ({placeholders})
                  AND  w.week_date < ?
            ) WHERE rn = 1
        ''', [keyword_id] + not_in_prev + [str(curr_week_date)]).fetchall():
            last_seen[hr['url']] = {'pos': hr['position'], 'date': str(hr['week_date'])}

    conn.close()

    seen_urls = set()
    out = []
    for r in rows:
        url      = r['url']
        is_dup   = url in seen_urls
        seen_urls.add(url)

        last_seen_pos  = None
        last_seen_date = None

        if is_dup:
            movement = 'duplicate'
            mv_val   = 0
        elif url not in prev_pos:
            if url in last_seen:
                movement       = 'returned'
                mv_val         = 0
                last_seen_pos  = last_seen[url]['pos']
                last_seen_date = last_seen[url]['date']
            else:
                movement = 'new'
                mv_val   = 0
        else:
            diff = prev_pos[url] - r['position']   # positive = moved up
            if diff == 0:
                movement = 'no_change'
            elif diff > 0:
                movement = f'up_{diff}'
            else:
                movement = f'down_{abs(diff)}'
            mv_val = diff

        out.append({
            'position':       r['position'],
            'url':            url,
            'title':          r['title']   or '',
            'snippet':        r['snippet'] or '',
            'sentiment':      r['sentiment'] or 'neutral',
            'owned':          bool(r['owned']) if r['owned'] is not None else False,
            'movement':       movement,
            'movement_val':   mv_val,
            'is_duplicate':   is_dup,
            'last_seen_pos':  last_seen_pos,
            'last_seen_date': last_seen_date,
        })

    return jsonify(out)


@app.route('/api/history')
def api_history():
    keyword_id = request.args.get('keyword_id', type=int)
    urls       = request.args.getlist('urls[]')

    if not keyword_id:
        return jsonify({})

    conn = get_db()

    if not urls:
        latest = conn.execute(
            'SELECT id FROM weeks ORDER BY week_date DESC LIMIT 1'
        ).fetchone()
        if not latest:
            conn.close()
            return jsonify({})
        top = conn.execute('''
            SELECT DISTINCT url FROM serp_results
            WHERE keyword_id = ? AND week_id = ?
            ORDER BY position LIMIT 10
        ''', (keyword_id, latest['id'])).fetchall()
        urls = [r['url'] for r in top]

    history = {}
    for url in urls:
        rows = conn.execute('''
            SELECT w.week_date, sr.position
            FROM   serp_results sr
            JOIN   weeks w ON sr.week_id = w.id
            WHERE  sr.keyword_id = ? AND sr.url = ?
            ORDER  BY w.week_date
        ''', (keyword_id, url)).fetchall()
        if rows:
            history[url] = [{'date': str(r['week_date']), 'position': r['position']}
                            for r in rows]

    conn.close()
    return jsonify(history)


@app.route('/api/weeks')
def api_weeks():
    conn  = get_db()
    weeks = conn.execute('SELECT * FROM weeks ORDER BY week_date DESC').fetchall()
    conn.close()
    return jsonify([{'id': w['id'], 'week_date': str(w['week_date'])} for w in weeks])


@app.route('/api/tag', methods=['POST'])
def api_tag():
    data      = request.get_json(force=True)
    url       = data.get('url', '').strip()
    sentiment = data.get('sentiment')
    notes     = data.get('notes', '')
    owned     = data.get('owned')   # None means "don't change it"

    if not url:
        return jsonify({'error': 'url required'}), 400
    if sentiment is not None and sentiment not in ('positive', 'negative', 'neutral'):
        return jsonify({'error': 'Invalid sentiment'}), 400

    conn = get_db()

    if sentiment is not None and owned is not None:
        conn.execute('''
            INSERT INTO url_tags (url, sentiment, notes, owned) VALUES (?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET sentiment=excluded.sentiment, notes=excluded.notes, owned=excluded.owned
        ''', (url, sentiment, notes, int(owned)))
    elif sentiment is not None:
        conn.execute('''
            INSERT INTO url_tags (url, sentiment, notes) VALUES (?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET sentiment=excluded.sentiment, notes=excluded.notes
        ''', (url, sentiment, notes))
    elif owned is not None:
        conn.execute('''
            INSERT INTO url_tags (url, sentiment, notes, owned) VALUES (?, "neutral", "", ?)
            ON CONFLICT(url) DO UPDATE SET owned=excluded.owned
        ''', (url, int(owned)))

    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/api/upload', methods=['POST'])
def api_upload():
    week_date = request.form.get('week_date', '').strip()
    files     = request.files.getlist('csvfiles')

    if not week_date:
        return jsonify({'error': 'week_date is required'}), 400
    if not files or all(f.filename == '' for f in files):
        return jsonify({'error': 'No files provided'}), 400

    try:
        datetime.strptime(week_date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'week_date must be YYYY-MM-DD'}), 400

    from importer import import_csv_uploads
    try:
        count = import_csv_uploads(DB_PATH, files, week_date)
        # Re-scan XLSX reports for any updated color tags
        try:
            from scan_colors import scan_and_tag
            scan_and_tag(DB_PATH, verbose=False)
        except Exception:
            pass
        return jsonify({'ok': True, 'imported': count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/compare')
def api_compare():
    """Return side-by-side results for two weeks."""
    keyword_id = request.args.get('keyword_id', type=int)
    week_a_id  = request.args.get('week_a',     type=int)
    week_b_id  = request.args.get('week_b',     type=int)

    if not all([keyword_id, week_a_id, week_b_id]):
        return jsonify({'error': 'keyword_id, week_a, and week_b are required'}), 400

    conn = get_db()

    def get_week_results(week_id):
        rows = conn.execute('''
            SELECT sr.position, sr.url, sr.title, ut.sentiment, ut.owned
            FROM   serp_results sr
            LEFT JOIN url_tags ut ON sr.url = ut.url
            WHERE  sr.keyword_id = ? AND sr.week_id = ?
            ORDER  BY sr.position
        ''', (keyword_id, week_id)).fetchall()
        return {r['url']: {'position': r['position'], 'title': r['title'] or '', 'sentiment': r['sentiment'] or 'neutral', 'owned': bool(r['owned']) if r['owned'] is not None else False}
                for r in rows}

    week_a_date = conn.execute('SELECT week_date FROM weeks WHERE id=?', (week_a_id,)).fetchone()
    week_b_date = conn.execute('SELECT week_date FROM weeks WHERE id=?', (week_b_id,)).fetchone()

    a = get_week_results(week_a_id)
    b = get_week_results(week_b_id)
    conn.close()

    all_urls = list(dict.fromkeys(list(a.keys()) + list(b.keys())))

    rows = []
    for url in all_urls:
        pos_a = a[url]['position'] if url in a else None
        pos_b = b[url]['position'] if url in b else None
        title = (a.get(url) or b.get(url) or {}).get('title', '')
        sentiment = (a.get(url) or b.get(url) or {}).get('sentiment', 'neutral')

        owned = (a.get(url) or b.get(url) or {}).get('owned', False)

        if pos_a is not None and pos_b is not None:
            diff = pos_a - pos_b   # positive = moved up from A to B
        else:
            diff = None

        rows.append({
            'url':       url,
            'title':     title,
            'sentiment': sentiment,
            'owned':     owned,
            'pos_a':     pos_a,
            'pos_b':     pos_b,
            'diff':      diff,
        })

    rows.sort(key=lambda r: (r['pos_b'] is None, r['pos_b'] or 9999))

    return jsonify({
        'week_a': str(week_a_date['week_date']) if week_a_date else '',
        'week_b': str(week_b_date['week_date']) if week_b_date else '',
        'rows':   rows,
    })


@app.route('/api/volatility')
def api_volatility():
    """Return week-over-week average position change per keyword."""
    keyword_id = request.args.get('keyword_id', type=int)

    conn = get_db()

    kw_filter = 'AND sr.keyword_id = ?' if keyword_id else ''
    params = (keyword_id,) if keyword_id else ()

    weeks = conn.execute(
        'SELECT id, week_date FROM weeks ORDER BY week_date'
    ).fetchall()

    keywords = conn.execute('SELECT id, name FROM keywords ORDER BY name').fetchall()

    result = {}
    for kw in keywords:
        if keyword_id and kw['id'] != keyword_id:
            continue
        kw_data = []
        prev_positions = {}
        for i, week in enumerate(weeks):
            curr = {
                r['url']: r['position']
                for r in conn.execute(
                    'SELECT url, position FROM serp_results WHERE keyword_id=? AND week_id=?',
                    (kw['id'], week['id'])
                ).fetchall()
            }
            if prev_positions and curr:
                shared = set(prev_positions) & set(curr)
                if shared:
                    avg_change = sum(abs(curr[u] - prev_positions[u]) for u in shared) / len(shared)
                    kw_data.append({'date': str(week['week_date']), 'volatility': round(avg_change, 2)})
                else:
                    kw_data.append({'date': str(week['week_date']), 'volatility': None})
            prev_positions = curr
        result[kw['name']] = kw_data

    conn.close()
    return jsonify(result)



@app.route('/api/fetch', methods=['POST'])
def api_fetch():
    data      = request.get_json(force=True)
    week_date = data.get('week_date', '').strip()

    if not week_date:
        return jsonify({'error': 'week_date required'}), 400
    try:
        datetime.strptime(week_date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'week_date must be YYYY-MM-DD'}), 400

    api_key = os.environ.get('SCALESERP_API_KEY', '')
    if not api_key:
        return jsonify({'error': 'SCALESERP_API_KEY environment variable is not set'}), 500

    from fetcher import fetch_all
    results = fetch_all(week_date, DB_PATH, api_key)

    total  = sum(v['count'] for v in results.values())
    errors = {k: v['error'] for k, v in results.items() if v['error']}

    return jsonify({'ok': True, 'imported': total, 'results': results, 'errors': errors})


@app.route('/api/fetch_status')
def api_fetch_status():
    """Check whether SCALESERP_API_KEY is configured."""
    configured = bool(os.environ.get('SCALESERP_API_KEY', ''))
    return jsonify({'configured': configured})


@app.route('/api/mozcast')
def api_mozcast():
    """Fetch MozCast temperature data."""
    import urllib.request
    import re
    try:
        req = urllib.request.Request(
            'https://moz.com/mozcast/',
            headers={'User-Agent': 'Mozilla/5.0 (compatible; SERP-Dashboard/1.0)'}
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            html = resp.read().decode('utf-8', errors='ignore')

        # Extract temperature data from the page
        # MozCast embeds chart data as JSON in a script tag
        temp_pattern = re.search(r'"temperatures"\s*:\s*(\[.*?\])', html, re.DOTALL)
        date_pattern = re.search(r'"dates"\s*:\s*(\[.*?\])', html, re.DOTALL)

        if temp_pattern and date_pattern:
            import json as _json
            temps = _json.loads(temp_pattern.group(1))
            dates = _json.loads(date_pattern.group(1))
            data = [{'date': d, 'temp': t} for d, t in zip(dates, temps)]
            return jsonify({'ok': True, 'data': data})

        # Fallback: look for the current temperature display
        current = re.search(r'(\d+(?:\.\d+)?)\s*[°&deg;]', html)
        if current:
            return jsonify({'ok': True, 'data': [], 'current': float(current.group(1))})

        return jsonify({'ok': False, 'error': 'Could not parse MozCast data'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/stats')
def api_stats():
    keyword_id = request.args.get('keyword_id', type=int)
    week_id    = request.args.get('week_id',    type=int)

    if not keyword_id or not week_id:
        return jsonify({})

    conn = get_db()

    total = conn.execute(
        'SELECT COUNT(*) FROM serp_results WHERE keyword_id=? AND week_id=?',
        (keyword_id, week_id)
    ).fetchone()[0]

    # Green = positive + neutral (anything not flagged red)
    neg_count = conn.execute('''
        SELECT COUNT(*) FROM serp_results sr
        JOIN url_tags ut ON sr.url = ut.url
        WHERE sr.keyword_id=? AND sr.week_id=? AND ut.sentiment="negative"
    ''', (keyword_id, week_id)).fetchone()[0]

    pos_count = total - neg_count   # green = everything that isn't red

    prev = conn.execute('''
        SELECT id FROM weeks
        WHERE week_date < (SELECT week_date FROM weeks WHERE id=?)
        ORDER BY week_date DESC LIMIT 1
    ''', (week_id,)).fetchone()

    new_count = 0
    if prev:
        prev_urls = set(
            r[0] for r in conn.execute(
                'SELECT url FROM serp_results WHERE keyword_id=? AND week_id=?',
                (keyword_id, prev['id'])
            ).fetchall()
        )
        curr_urls = set(
            r[0] for r in conn.execute(
                'SELECT url FROM serp_results WHERE keyword_id=? AND week_id=?',
                (keyword_id, week_id)
            ).fetchall()
        )
        new_count = len(curr_urls - prev_urls)

    owned_count = conn.execute('''\
        SELECT COUNT(*) FROM serp_results sr
        JOIN url_tags ut ON sr.url = ut.url
        WHERE sr.keyword_id=? AND sr.week_id=? AND ut.owned=1
    ''', (keyword_id, week_id)).fetchone()[0]

    conn.close()
    return jsonify({
        'total':    total,
        'positive': pos_count,
        'negative': neg_count,
        'new':      new_count,
        'owned':    owned_count,
    })


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    init_db()

    conn = get_db()
    count = conn.execute('SELECT COUNT(*) FROM serp_results').fetchone()[0]
    conn.close()

    if count == 0:
        print('Database is empty — importing historical data. This may take a minute...', flush=True)
        from importer import import_all_history
        import_all_history(DB_PATH)
    else:
        print(f'Database ready ({count:,} rows).', flush=True)

    port = int(os.environ.get('PORT', 5050))
    app.run(debug=False, host='0.0.0.0', port=port)
