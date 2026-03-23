import openpyxl
import csv
import sqlite3
import os
import re
from datetime import datetime, date

HISTORY_DIR = '/Volumes/Creative/L/SEO/SERPs/*SERPs Full History'
CSV_DIR_2026 = '/Volumes/Creative/L/SEO/SERPs/2026/02-Feb/Feb 23'

# Maps CSV filename stem (lowercase) to display keyword name
# Covers all naming variations found across weekly folders
CSV_KEYWORD_MAP = {
    'frank':                   'Frank VanderSloot',
    'melaleuca.com':           'Melaleuca.com',
    'melaleuca':               'Melaleuca',
    'products':                'Melaleuca Products',
    'melaleuca products':      'Melaleuca Products',
    'reviews':                 'Melaleuca Reviews',
    'melaleuca reviews':       'Melaleuca Reviews',
    'riverbend ranch':         'Riverbend Ranch',
    'riverbendranch':          'Riverbend Ranch',
    'rbr':                     'Riverbend Ranch',
    'the wellness company':    'The Wellness Company',
    'thewellnesscompany':      'The Wellness Company',
    'twc':                     'The Wellness Company',
    'the wellnesscompany':     'The Wellness Company',
}

# Stems to silently skip (not part of the 7 tracked keywords)
CSV_SKIP_STEMS = {
    'frank-hawaii', 'frank hawaii', 'frankhawaii',
    'jerry',
    'rbr_bing', 'rbr_ddg', 'rbr_yahoo',
    'rbr bing', 'rbr ddg', 'rbr yahoo',
}

# All weekly CSV folders not covered by the history XLSX files
# Format: (folder_path, YYYY-MM-DD)
MISSING_WEEKS = [
    ('/Volumes/Creative/L/SEO/SERPs/2025/12-Dec/8-Dec',       '2025-12-08'),
    ('/Volumes/Creative/L/SEO/SERPs/2025/12-Dec/15-Dec',      '2025-12-15'),
    ('/Volumes/Creative/L/SEO/SERPs/2025/12-Dec/22-Dec',      '2025-12-22'),
    ('/Volumes/Creative/L/SEO/SERPs/2025/12-Dec/29-Dec',      '2025-12-29'),
    ('/Volumes/Creative/L/SEO/SERPs/2026/01-Jan/Jan 5',       '2026-01-05'),
    ('/Volumes/Creative/L/SEO/SERPs/2026/01-Jan/Jan 12',      '2026-01-12'),
    ('/Volumes/Creative/L/SEO/SERPs/2026/01-Jan/Jan 19',      '2026-01-19'),
    ('/Volumes/Creative/L/SEO/SERPs/2026/01-Jan/Jan 26',      '2026-01-26'),
    ('/Volumes/Creative/L/SEO/SERPs/2026/02-Feb/Feb 2',       '2026-02-02'),
    ('/Volumes/Creative/L/SEO/SERPs/2026/02-Feb/Feb 9',       '2026-02-09'),
    ('/Volumes/Creative/L/SEO/SERPs/2026/02-Feb/Feb 16',      '2026-02-16'),
    ('/Volumes/Creative/L/SEO/SERPs/2026/02-Feb/Feb 23',      '2026-02-23'),
    ('/Volumes/Creative/L/SEO/SERPs/2026/03-March/March 2',   '2026-03-02'),
]

# Maps history XLSX filename to keyword display name
HISTORY_FILE_MAP = {
    'Melaleuca_History.xlsx': 'Melaleuca',
    'Melaleuca.com_History.xlsx': 'Melaleuca.com',
    'Frank VanderSloot_History.xlsx': 'Frank VanderSloot',
    'Melaleuca Products_History.xlsx': 'Melaleuca Products',
    'Melaleuca Reviews_History.xlsx': 'Melaleuca Reviews',
    'The Wellness Company_History.xlsx': 'The Wellness Company',
    'RBR Google_History.xlsx': 'Riverbend Ranch',
}

def slugify(name):
    import re
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')

def get_or_create_keyword(conn, name):
    row = conn.execute('SELECT id FROM keywords WHERE name = ?', (name,)).fetchone()
    if row:
        return row[0]
    cur = conn.execute('INSERT INTO keywords (name, slug) VALUES (?, ?)', (name, slugify(name)))
    return cur.lastrowid

def get_or_create_week(conn, week_date):
    if isinstance(week_date, datetime):
        week_date = week_date.date()
    if isinstance(week_date, str):
        week_date = datetime.strptime(week_date, '%Y-%m-%d').date()
    ds = str(week_date)
    row = conn.execute('SELECT id FROM weeks WHERE week_date = ?', (ds,)).fetchone()
    if row:
        return row[0]
    cur = conn.execute('INSERT INTO weeks (week_date) VALUES (?)', (ds,))
    return cur.lastrowid

def normalize_url(url):
    """Strip Google tracking params (srsltid, etc.) so the same page isn't treated as new."""
    if not url:
        return url
    url = re.sub(r'[?&]srsltid=[^&]*', '', url)
    url = re.sub(r'[?&]srs=[^&]*',     '', url)
    return url.rstrip('?&')

def insert_result(conn, keyword_id, week_id, position, url, title, snippet):
    try:
        conn.execute(
            'INSERT OR IGNORE INTO serp_results (keyword_id, week_id, position, url, title, snippet) VALUES (?,?,?,?,?,?)',
            (keyword_id, week_id, position, normalize_url(url), title or '', snippet or '')
        )
    except Exception:
        pass

def import_history_xlsx(db_path, xlsx_path, keyword_name):
    """Parse a keyword history XLSX.
    Supports two formats:
      Format A (most keywords): date in col B as datetime object, position in col A
      Format B (The Wellness Company): date in header string 'Keyword - Google - M/D/YYYY'
    """
    print(f'  Importing {keyword_name} from {os.path.basename(xlsx_path)}...', flush=True)
    conn = sqlite3.connect(db_path)
    keyword_id = get_or_create_keyword(conn, keyword_name)

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    current_week_id = None
    imported = 0
    date_re = re.compile(r'(\d{1,2}/\d{1,2}/\d{4})')

    for row in ws.iter_rows(values_only=True):
        if not row or len(row) < 2:
            continue
        val_a = row[0]
        val_b = row[1]
        val_c = row[2] if len(row) > 2 else None
        val_d = row[3] if len(row) > 3 else None

        # Format A — date row: col A is None, col B is a datetime object
        if isinstance(val_b, (datetime, date)) and val_a is None:
            current_week_id = get_or_create_week(conn, val_b)
            continue

        # Format B — date row: col A is None, col B is a string containing M/D/YYYY
        if val_a is None and isinstance(val_b, str):
            m = date_re.search(val_b)
            if m:
                try:
                    d = datetime.strptime(m.group(1), '%m/%d/%Y').date()
                    current_week_id = get_or_create_week(conn, d)
                except ValueError:
                    pass
            continue

        # Skip page-separator and header rows
        if val_b is None:
            continue
        if isinstance(val_b, str) and (
            val_b.startswith('Google SERP') or
            val_b.startswith('Page ') or
            val_b == 'Current URL'
        ):
            continue

        # Result row: col A = absolute position (int), col B = URL
        if isinstance(val_a, int) and isinstance(val_b, str) and val_b.startswith('http') and current_week_id:
            insert_result(conn, keyword_id, current_week_id, val_a, val_b, val_c, val_d)
            imported += 1

    conn.commit()
    conn.close()
    wb.close()
    print(f'    -> {imported} rows', flush=True)
    return imported

def import_csv_file(db_path, filepath, keyword_name, week_date_str):
    """Parse a ScaleSerp CSV file. Positions reset per Google page; we assign absolute positions."""
    conn = sqlite3.connect(db_path)
    keyword_id = get_or_create_keyword(conn, keyword_name)
    week_id = get_or_create_week(conn, week_date_str)

    # Replace any existing data for this keyword+week so re-uploads don't duplicate
    conn.execute(
        'DELETE FROM serp_results WHERE keyword_id=? AND week_id=?',
        (keyword_id, week_id)
    )

    imported = 0
    abs_position = 0
    prev_pos = 999

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pos_str = row.get('organic_results.position', '').strip()
            url = row.get('organic_results.link', '').strip()
            title = row.get('organic_results.title', '').strip()
            snippet = row.get('organic_results.snippet', '').strip()

            if not pos_str or not url:
                continue

            try:
                pos = int(pos_str)
            except ValueError:
                continue

            # When position resets to 1 (new Google page), abs_position continues
            abs_position += 1
            prev_pos = pos

            insert_result(conn, keyword_id, week_id, abs_position, url, title, snippet)
            imported += 1

    conn.commit()
    conn.close()
    return imported

def import_csv_dir(db_path, csv_dir, week_date_str):
    """Import all recognized CSVs from a folder for a given date."""
    total = 0
    for filename in sorted(os.listdir(csv_dir)):
        if not filename.lower().endswith('.csv'):
            continue
        stem = os.path.splitext(filename)[0].lower()
        if stem in CSV_SKIP_STEMS:
            continue
        keyword_name = CSV_KEYWORD_MAP.get(stem)
        if not keyword_name:
            print(f'  Skipping unrecognized CSV: {filename}')
            continue
        filepath = os.path.join(csv_dir, filename)
        count = import_csv_file(db_path, filepath, keyword_name, week_date_str)
        print(f'  {keyword_name}: {count} rows ({filename})', flush=True)
        total += count
    return total


def import_missing_weeks(db_path):
    """Import all weekly CSV folders not covered by the history XLSX files."""
    conn = sqlite3.connect(db_path)
    existing = set(
        r[0] for r in conn.execute('SELECT week_date FROM weeks').fetchall()
    )
    conn.close()

    for folder, date_str in MISSING_WEEKS:
        if date_str in existing:
            print(f'  Already have {date_str} — skipping')
            continue
        if not os.path.isdir(folder):
            print(f'  WARNING: folder not found: {folder}')
            continue
        print(f'  Importing {date_str} from {folder}')
        import_csv_dir(db_path, folder, date_str)

def import_csv_uploads(db_path, files, week_date_str):
    """Import uploaded FileStorage objects (from Flask)."""
    import tempfile
    total = 0
    for f in files:
        stem = os.path.splitext(f.filename)[0].lower()
        keyword_name = CSV_KEYWORD_MAP.get(stem)
        if not keyword_name:
            print(f'  Skipping unknown upload: {f.filename}')
            continue
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            f.save(tmp.name)
            count = import_csv_file(db_path, tmp.name, keyword_name, week_date_str)
            os.unlink(tmp.name)
        print(f'  {keyword_name}: {count} rows', flush=True)
        total += count
    return total

def import_all_history(db_path):
    """Run full history import: XLSX history files + Feb 23 2026 CSVs."""
    print('=== Importing XLSX history files ===', flush=True)
    for filename, keyword_name in HISTORY_FILE_MAP.items():
        filepath = os.path.join(HISTORY_DIR, filename)
        if os.path.exists(filepath):
            import_history_xlsx(db_path, filepath, keyword_name)
        else:
            print(f'  WARNING: Not found: {filepath}')

    print('=== Importing missing weekly CSVs (Dec 2025 – Feb 2026) ===', flush=True)
    import_missing_weeks(db_path)

    print('=== Import complete ===', flush=True)
