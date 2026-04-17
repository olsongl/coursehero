#!/usr/bin/env python3
"""
Scrape ALL CourseHero documents for Ohio State University
BCHM 4511 / BIOCHEM 451 / MOLGEN 4500 — Biochemistry.

Requires Chrome running with --remote-debugging-port=9222 (auto-launched if missing).
Iterates all paginated sitemap pages (?p=1, ?p=2 ...) for each course URL.
Saves raw HTML per page and combined JSON of all extracted document records.
Tracks URL scrape status: success, cloudflare_block, timeout, wifi_loss.

Target: ALL ~384 documents across 3 course URLs — no type filtering applied.
  - MOLGEN 4500:  ~223 documents
  - BIOCHEM 451:  ~158 documents
  - BIOCHEM 4511: ~3 documents

Unavailable/failed document links are marked as "(not available)".

FEATURES:
  - Automatic retry logic with exponential backoff for failed page loads
  - Tracks failed URLs separately and logs retry attempts
  - Resilient to WiFi dropouts and temporary Cloudflare blocks
  - Reports final statistics on retries, successes, and failures
  - URL status tracking for Excel summary (success / cloudflare / timeout / wifi)
  - Grabs every single document URL across all paginated pages (?p=1, ?p=2, ...)

Usage:
    source .venv/bin/activate  (from workspace root)
    cd "COURSEHERO SEARCHES_CoPilot/Coursehero OSU BCHM 4511"
    python3 scrape_osu_bchm4511_with_retry_grab_all.py
"""

import requests
import json
import time
import re
import os
import sys
import subprocess
import math
import websocket
from bs4 import BeautifulSoup
from datetime import datetime
from collections import Counter

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CDP_BASE   = "http://localhost:9222"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
WAIT_SECS  = 9         # seconds to wait for JS render (generous for CourseHero)
BETWEEN_PAGES = 3      # extra pause between consecutive page requests (rate limit)
DATE_STAMP = datetime.now().strftime('%Y-%m-%d')

# Retry configuration
MAX_RETRIES        = 3   # max retry attempts per URL
INITIAL_BACKOFF    = 2   # base backoff in seconds (exponential: 2, 4, 8)
CLOUDFLARE_BACKOFF = 5   # extra backoff for Cloudflare 429 errors

# OSU BCHM 4511 / BIOCHEM 451 / MOLGEN 4500 — three course sitemap URLs
COURSE_URLS = [
    (
        "MOLGEN 4500",
        "BIOCHEM",
        "https://www.coursehero.com/sitemap/schools/105-Ohio-State-University/courses/1570875-MOLGEN4500/",
    ),
    (
        "BIOCHEM 451",
        "BIOCHEM",
        "https://www.coursehero.com/sitemap/schools/105-Ohio-State-University/courses/10996388-BIOCHEM451/",
    ),
    (
        "BIOCHEM 4511",
        "BIOCHEM",
        "https://www.coursehero.com/sitemap/schools/105-Ohio-State-University/courses/1652614-BIOCHEM4511/",
    ),
]


# ---------------------------------------------------------------------------
# URL Status Tracker
# ---------------------------------------------------------------------------
class URLStatusTracker:
    """Tracks URL scrape status for each course sitemap URL."""
    def __init__(self):
        self.url_status = {}  # {url: {"status": "success|cloudflare|timeout|wifi", "error": "..."}}

    def set_status(self, url, status, error=""):
        """Set status for a URL."""
        self.url_status[url] = {"status": status, "error": error}

    def get_status_dict(self):
        """Return dict suitable for JSON export."""
        return self.url_status


# ---------------------------------------------------------------------------
# Retry tracker
# ---------------------------------------------------------------------------
class RetryTracker:
    """Tracks retry statistics."""
    def __init__(self):
        self.total_attempts   = 0
        self.successful_loads = 0
        self.failed_urls      = []
        self.retry_log        = []

    def log_retry(self, url, attempt, error, backoff_secs):
        msg = f"Retry {attempt}/{MAX_RETRIES} for {url} after {backoff_secs}s backoff. Error: {error}"
        self.retry_log.append(msg)
        print(f"      [RETRY {attempt}] {msg[:80]}...")

    def log_failure(self, url, error):
        msg = f"FAILED after {MAX_RETRIES} attempts: {url}"
        self.failed_urls.append({"url": url, "error": str(error)})
        print(f"      [FAILED] {msg}")

    def log_success(self):
        self.successful_loads += 1


def retry_open_tab(url, tracker=None):
    """Open tab and navigate, retrying with exponential backoff on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            html, tab_id, ws, ws_url = get_page_html(url)
            if tracker:
                tracker.log_success()
            return html, tab_id, ws, ws_url
        except Exception as e:
            error_msg = str(e)
            is_cloudflare = "429" in error_msg or "cloudflare" in error_msg.lower()
            base_backoff  = CLOUDFLARE_BACKOFF if is_cloudflare else INITIAL_BACKOFF
            backoff_secs  = base_backoff * (2 ** (attempt - 1))

            if attempt < MAX_RETRIES:
                if tracker:
                    tracker.log_retry(url, attempt, error_msg, backoff_secs)
                time.sleep(backoff_secs)
            else:
                if tracker:
                    tracker.log_failure(url, error_msg)
                raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {error_msg}")


# ---------------------------------------------------------------------------
# CDP helpers
# ---------------------------------------------------------------------------
def ensure_chrome_debug():
    """Check if Chrome is listening on 9222; if not, launch a debug instance."""
    try:
        r = requests.get(f"{CDP_BASE}/json/version", timeout=4)
        if r.status_code == 200:
            print("[Chrome] Already listening on port 9222.")
            return True
    except Exception:
        pass

    print("[Chrome] Not found on port 9222 — launching debug instance...")
    chrome_paths = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    user_data_dir = os.path.expanduser("~/Library/Application Support/Google/Chrome-Debug")
    os.makedirs(user_data_dir, exist_ok=True)

    for chrome in chrome_paths:
        if os.path.exists(chrome):
            subprocess.Popen([
                chrome,
                "--remote-debugging-port=9222",
                f"--user-data-dir={user_data_dir}",
                "--no-first-run",
                "--disable-default-apps",
                "about:blank"
            ])
            time.sleep(4)
            try:
                r = requests.get(f"{CDP_BASE}/json/version", timeout=8)
                if r.status_code == 200:
                    print("[Chrome] Debug instance launched successfully.")
                    return True
            except Exception:
                pass
    raise RuntimeError(
        "Could not connect to Chrome on port 9222. "
        "Please launch Chrome manually with --remote-debugging-port=9222."
    )


def get_page_html(url, wait=WAIT_SECS, tracker=None):
    """Open a new Chrome tab via CDP, navigate to URL, return (html, tab_id, ws_url)."""
    resp = requests.put(f"{CDP_BASE}/json/new?{url}", timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"CDP new tab failed: {resp.status_code}")
    tab    = resp.json()
    tab_id = tab['id']
    ws_url = tab['webSocketDebuggerUrl']

    time.sleep(2)
    ws = websocket.create_connection(ws_url, timeout=30, suppress_origin=True)

    ws.send(json.dumps({"id": 1, "method": "Page.navigate", "params": {"url": url}}))
    ws.recv()

    print(f"    Waiting {wait}s for JS render...")
    time.sleep(wait)

    html = _get_html_from_ws(ws)
    return html, tab_id, ws, ws_url


def _get_html_from_ws(ws):
    """Read document.documentElement.outerHTML from an open WebSocket."""
    ws.send(json.dumps({
        "id": 2,
        "method": "Runtime.evaluate",
        "params": {"expression": "document.documentElement.outerHTML"}
    }))
    result = json.loads(ws.recv())
    return result.get('result', {}).get('result', {}).get('value', '')


def click_next_and_get_html(ws, wait=WAIT_SECS):
    """
    Click the "Next" pagination button on the currently open page.
    Returns (html_after_click, clicked_ok).
    Uses JS: find li containing span with text 'Next' that has cursor-pointer class.
    """
    js_click = """
(function() {
  var items = Array.from(document.querySelectorAll('li'));
  var nextBtn = items.find(function(el) {
    var span = el.querySelector('span');
    var txt = span ? span.textContent.trim() : '';
    var cls = el.className || '';
    return txt === 'Next' && cls.indexOf('cursor-pointer') !== -1;
  });
  if (nextBtn) {
    nextBtn.click();
    return true;
  }
  return false;
})()
"""
    ws.send(json.dumps({
        "id": 10,
        "method": "Runtime.evaluate",
        "params": {"expression": js_click}
    }))
    result = json.loads(ws.recv())
    clicked = result.get('result', {}).get('result', {}).get('value', False)

    if not clicked:
        return '', False

    print(f"    Clicked Next — waiting {wait}s for JS render...")
    time.sleep(wait)
    html = _get_html_from_ws(ws)
    return html, True


def close_tab(tab_id, ws):
    """Close the CDP tab cleanly."""
    try:
        ws.send(json.dumps({"id": 99, "method": "Page.close"}))
    except Exception:
        pass
    try:
        ws.close()
    except Exception:
        pass
    try:
        requests.get(f"{CDP_BASE}/json/close/{tab_id}", timeout=5)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def parse_pagination(html):
    """
    Parse 'Showing X to Y of Z' text.
    Returns (total_docs, items_per_page).
    """
    m = re.search(r'Showing\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d[\d,]*)', html)
    if m:
        start    = int(m.group(1))
        end      = int(m.group(2))
        total    = int(m.group(3).replace(',', ''))
        per_page = end - start + 1
        return total, per_page
    soup = BeautifulSoup(html, 'lxml')
    p = soup.find('p', class_='tl_resourceContent_title')
    if p:
        m2 = re.search(r'of\s+(\d[\d,]*)', p.get_text())
        if m2:
            return int(m2.group(1).replace(',', '')), 30
    return 0, 30


def extract_documents(html, course_label, dept_label, base_url, page_num):
    """
    Parse ALL document card entries from a course sitemap page — no type filtering.
    Handles both new Tailwind and old legacy CourseHero formats.
    Returns list of dicts with all available metadata.
    Unavailable/broken links are included as "(not available)".
    """
    soup = BeautifulSoup(html, 'lxml')

    new_items = soup.find_all('li', attrs={'aria-label': re.compile(r'^(documents|trending)-\d+')})
    old_items = soup.find_all('li', class_='tl_documents_list-item')

    results = []
    seen    = set()

    # ---- NEW Tailwind format ----
    for item in new_items:
        a_tag = item.find('a', href=lambda h: h and '/file/' in h)
        if not a_tag:
            any_link = item.find('a', href=True)
            if any_link:
                href = any_link['href']
                if href.startswith('/'):
                    href = 'https://www.coursehero.com' + href
                file_url = "(not available)"
            else:
                continue
        else:
            href = a_tag['href']
            if href.startswith('/'):
                href = 'https://www.coursehero.com' + href
            file_url = href.rstrip('/')

        if file_url in seen and file_url != "(not available)":
            continue
        if file_url != "(not available)":
            seen.add(file_url)

        h3    = item.find('h3')
        title = ''
        if h3:
            title = h3.get_text(strip=True)
        elif a_tag:
            title = a_tag.get('title', '').split('/')[-2].replace('-', ' ')

        footer      = item.find('footer')
        course_code = ''
        school      = 'Ohio State University'
        if footer:
            h4 = footer.find('h4')
            if h4:
                course_code = h4.get_text(strip=True)
            school_div = footer.find('div', class_=re.compile(r'tw-truncate'))
            if school_div:
                school = school_div.get_text(strip=True) or school

        slug          = file_url.split('/file/')[-1] if '/file/' in file_url else ''
        semester_year = _parse_semester(title) or _parse_semester(slug.replace('-', ' '))
        file_ext      = _get_ext(title) or _get_ext(file_url) or 'pdf'

        pages     = ''
        page_span = item.find('span', string=re.compile(r'\d+\s+page', re.I))
        if page_span:
            pm = re.search(r'(\d+)', page_span.get_text())
            if pm:
                pages = pm.group(1)

        results.append({
            "title":         title,
            "url":           file_url,
            "course_label":  course_label,
            "dept_label":    dept_label,
            "course_code":   course_code,
            "course_name":   '',
            "semester_year": semester_year,
            "pages":         pages,
            "file_ext":      file_ext,
            "school":        school,
            "source_url":    base_url,
            "page_scraped":  page_num,
        })

    # ---- OLD legacy format (fallback) ----
    for item in old_items:
        file_url = ''
        for a in item.find_all('a', href=True):
            href = a['href']
            if href.startswith('/file/'):
                file_url = 'https://www.coursehero.com' + href.rstrip('/')
                break
        if not file_url:
            reg_a = item.find('a', href=re.compile(r'/register/\?get_doc='))
            if reg_a:
                m = re.search(r'get_doc=(\d+)', reg_a['href'])
                if m:
                    file_url = f'https://www.coursehero.com/file/{m.group(1)}/'
        if not file_url:
            file_url = "(not available)"

        if file_url in seen and file_url != "(not available)":
            continue
        if file_url != "(not available)":
            seen.add(file_url)

        title_li = item.find('li', class_=re.compile(r'ch_product_document_title'))
        title    = title_li.get_text(strip=True) if title_li else ''
        if not title:
            footer_div = item.find('div', class_='ch_product_document_footer')
            title = footer_div.get_text(strip=True) if footer_div else ''

        pages_span = item.find('span', class_='ch_product_document_count')
        pages      = pages_span.get_text(strip=True) if pages_span else ''

        school_li = item.find('li', class_='ch_product_document_meta-school')
        school    = school_li.get_text(strip=True) if school_li else 'Ohio State University'

        course_name_li = item.find('li', class_='ch_product_document_meta-course-name')
        course_name    = course_name_li.get_text(strip=True) if course_name_li else ''

        meta_li       = item.find('li', class_='meta-course_nosnippet')
        course_code   = ''
        semester_year = ''
        if meta_li:
            raw = re.sub(r'\s+', ' ', meta_li.get_text()).strip()
            mm  = re.match(r'([A-Z]+\s*\d+[A-Z]*)\s*[-–]\s*(.*)', raw)
            if mm:
                course_code   = mm.group(1).strip()
                semester_year = mm.group(2).strip()
            else:
                course_code = raw

        file_ext = _get_ext(title) or _get_ext(file_url) or 'pdf'

        results.append({
            "title":         title,
            "url":           file_url,
            "course_label":  course_label,
            "dept_label":    dept_label,
            "course_code":   course_code,
            "course_name":   course_name,
            "semester_year": semester_year,
            "pages":         pages,
            "file_ext":      file_ext,
            "school":        school,
            "source_url":    base_url,
            "page_scraped":  page_num,
        })

    return results


def _parse_semester(text):
    """Extract semester/year string from text."""
    if not text:
        return ''
    m = re.search(r'\b(Fall|Spring|Winter|Summer|Sp|Fl|Wi|Su)\s*(20\d{2}|1[89]\d{2})\b', text, re.I)
    if m:
        sem = m.group(1).lower()
        yr  = m.group(2)
        sem_map = {
            'fall': 'Fall', 'fl': 'Fall',
            'spring': 'Spring', 'sp': 'Spring',
            'winter': 'Winter', 'wi': 'Winter',
            'summer': 'Summer', 'su': 'Summer',
        }
        return f"{sem_map.get(sem, sem.title())} {yr}"
    m2 = re.search(r'\b([FfWwSs])(\d{2})\b', text)
    if m2:
        p  = m2.group(1).lower()
        yr = '20' + m2.group(2)
        pm = {'f': 'Fall', 'w': 'Winter', 's': 'Spring'}
        return f"{pm.get(p, '?')} {yr}"
    m3 = re.search(r'\b(20\d{2}|19\d{2})\b', text)
    if m3:
        return m3.group(1)
    return ''


def _get_ext(text):
    """Extract file extension from text/URL."""
    m = re.search(r'\.(pdf|docx?|pptx?|xlsx?|txt|png|jpg|jpeg|gif|zip)(?:$|[^a-z])', text, re.I)
    return m.group(1).lower() if m else ''


# ---------------------------------------------------------------------------
# Main scrape loop — click-based pagination
# ---------------------------------------------------------------------------
def scrape_course(course_label, dept_label, base_url, tracker, url_status):
    """
    Scrape ALL pages of the course sitemap using click-based pagination.
    CourseHero sitemap pages use JavaScript pagination triggered by clicking
    "Next" — URL ?p=N changes are ignored by the client-side router, so we
    stay in a single tab and click the Next button to advance pages.
    Returns every document record found.
    """
    print(f"\n{'='*70}")
    print(f"Scraping: {course_label} ({dept_label})")
    print(f"  URL: {base_url}")

    all_docs  = []
    seen_urls = set()

    # --- Open tab and load page 1 ---
    url_p1 = base_url
    print(f"  Opening tab → {url_p1}")

    try:
        html, tab_id, ws, ws_url = retry_open_tab(url_p1, tracker=tracker)
    except RuntimeError as e:
        error_str = str(e).lower()
        if "429" in error_str or "cloudflare" in error_str:
            status = "cloudflare"
            url_status.set_status(base_url, status, "Cloudflare block (429)")
        elif "timeout" in error_str:
            status = "timeout"
            url_status.set_status(base_url, status, "Timeout")
        else:
            status = "wifi"
            url_status.set_status(base_url, status, str(e)[:60])
        print(f"  SKIPPING {course_label} — could not load page 1: {e}")
        return all_docs

    safe_label = course_label.replace(' ', '_').replace('/', '-')

    # Save page 1 HTML
    html_file = os.path.join(OUTPUT_DIR, f"raw_OSU_{safe_label}_p1.html")
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  Saved HTML → {os.path.basename(html_file)}")

    total, per_page = parse_pagination(html)
    print(f"  Total documents: {total} ({per_page} per page)")

    # Extract page 1 docs
    docs = extract_documents(html, course_label, dept_label, base_url, 1)
    for d in docs:
        if d['url'] not in seen_urls or d['url'] == "(not available)":
            seen_urls.add(d['url'])
            all_docs.append(d)
    print(f"  Page 1: {len(docs)} docs extracted (running total: {len(all_docs)})")

    if total == 0 and not docs:
        print("  No documents found — skipping course.")
        close_tab(tab_id, ws)
        url_status.set_status(base_url, "success", "")
        return all_docs

    total_pages = math.ceil(total / per_page) if per_page > 0 else 1
    print(f"  Pages to scrape: {total_pages}")

    # --- Click-based pagination: pages 2..N ---
    for page in range(2, total_pages + 1):
        print(f"  Page {page}/{total_pages} — clicking Next...")
        time.sleep(BETWEEN_PAGES)

        try:
            html_n, clicked = click_next_and_get_html(ws)
        except Exception as e:
            print(f"  Error clicking Next on page {page}: {e} — stopping pagination.")
            all_docs.append({
                "title":         f"[Page {page} failed to load]",
                "url":           "(not available)",
                "course_label":  course_label,
                "dept_label":    dept_label,
                "course_code":   "",
                "course_name":   "",
                "semester_year": "",
                "pages":         "",
                "file_ext":      "",
                "school":        "Ohio State University",
                "source_url":    base_url,
                "page_scraped":  page,
            })
            break

        if not clicked:
            print(f"  Next button not found or disabled at page {page} — pagination complete.")
            break

        # Save page N HTML
        html_file_n = os.path.join(OUTPUT_DIR, f"raw_OSU_{safe_label}_p{page}.html")
        with open(html_file_n, 'w', encoding='utf-8') as f:
            f.write(html_n)

        docs_n = extract_documents(html_n, course_label, dept_label, base_url, page)
        added  = 0
        for d in docs_n:
            if d['url'] not in seen_urls or d['url'] == "(not available)":
                seen_urls.add(d['url'])
                all_docs.append(d)
                added += 1
        print(f"  Page {page}: {len(docs_n)} extracted, {added} new (running total: {len(all_docs)})")

        if not docs_n:
            print(f"  No documents on page {page} — stopping pagination.")
            break

    close_tab(tab_id, ws)
    url_status.set_status(base_url, "success", "")
    return all_docs


def main():
    ensure_chrome_debug()

    all_documents = []
    seen_global   = set()
    tracker       = RetryTracker()
    url_status    = URLStatusTracker()

    for course_label, dept_label, base_url in COURSE_URLS:
        try:
            docs = scrape_course(course_label, dept_label, base_url, tracker, url_status)
        except Exception as e:
            print(f"  FATAL ERROR scraping {course_label}: {e}")
            error_str = str(e).lower()
            if "429" in error_str or "cloudflare" in error_str:
                url_status.set_status(base_url, "cloudflare", str(e)[:60])
            elif "timeout" in error_str:
                url_status.set_status(base_url, "timeout", str(e)[:60])
            else:
                url_status.set_status(base_url, "wifi", str(e)[:60])
            docs = []

        for d in docs:
            if d['url'] not in seen_global or d['url'] == "(not available)":
                seen_global.add(d['url'])
                all_documents.append(d)

        # Checkpoint after each course
        checkpoint_file = os.path.join(OUTPUT_DIR, f"osu_bchm4511_docs_{DATE_STAMP}_checkpoint.json")
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(all_documents, f, indent=2, ensure_ascii=False)
        print(f"  [Checkpoint] {len(all_documents)} total docs saved.")

    # Final save with URL status embedded
    out_file = os.path.join(OUTPUT_DIR, f"osu_bchm4511_docs_{DATE_STAMP}.json")
    export_data = {
        "documents":  all_documents,
        "url_status": url_status.get_status_dict()
    }
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*70}")
    print(f"DONE. {len(all_documents)} documents saved to:")
    print(f"  {out_file}")

    # Per-course summary
    course_counts = Counter(d['course_label'] for d in all_documents)
    print("\nDocuments per course:")
    for course, count in sorted(course_counts.items()):
        print(f"  {course}: {count}")

    # Not-available count
    na_count = sum(1 for d in all_documents if d['url'] == "(not available)")
    if na_count:
        print(f"\n  Records marked '(not available)': {na_count}")

    # Retry statistics
    print(f"\n{'='*70}")
    print("RETRY STATISTICS:")
    print(f"  Successful page loads: {tracker.successful_loads}")
    print(f"  Failed URLs (after {MAX_RETRIES} retries): {len(tracker.failed_urls)}")
    if tracker.failed_urls:
        print("\n  Failed URLs:")
        for item in tracker.failed_urls:
            print(f"    - {item['url']}")
            print(f"      Error: {item['error'][:100]}")
    if tracker.retry_log:
        print(f"\n  Total retry attempts logged: {len(tracker.retry_log)}")

    # URL status summary
    print(f"\n{'='*70}")
    print("URL SCRAPE STATUS:")
    for url, status_info in url_status.get_status_dict().items():
        status = status_info['status']
        error  = status_info.get('error', '')
        if status == 'success':
            print(f"  ✓ {status}: {url}")
        else:
            print(f"  ✗ {status}: {url}")
            if error:
                print(f"    Error: {error}")

    print(f"\nNext step: run generate_osu_bchm4511_xlsx.py to produce the Excel inventory.")


if __name__ == '__main__':
    main()
