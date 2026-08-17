#!/usr/bin/env python3
import argparse
import datetime as dt
import gzip
import json
import os
import random
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import quote, quote_plus, urljoin, urlparse, parse_qs

import requests
from lxml import html as lxml_html
from scrapling.fetchers import Fetcher

BASE = "https://mlbpark.donga.com/mp/b.php"

# ==========================================
# Domain Exceptions
# ==========================================
class TransientFetchError(Exception):
    """Network timeouts, 5xx errors -> retry"""
    pass

class CrawlBlockedError(Exception):
    """A 403/429 is a run-wide stop signal, not an item retry."""
    def __init__(self, url, status, retry_after=None):
        self.url = url
        self.status = status
        self.retry_after = retry_after
        super().__init__(f"crawl blocked status={status} retry_after={retry_after} url={url}")

class UnexpectedPageShape(Exception):
    """Broken DOM, login page redirect, HTML parsing bug -> item failed"""
    pass

class InvalidPostIdentity(Exception):
    """post_id missing or corrupted or redirected -> item failed"""
    pass

class InvariantViolation(Exception):
    """Logical impossibility reached -> FATAL"""
    pass

class Bug(Exception):
    """Code errors, syntax, typing -> FATAL"""
    pass


# ==========================================
# Helpers
# ==========================================
def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat()

def clean(text):
    if text is None:
        return None
    return re.sub(r"\s+", " ", str(text)).strip()

def ints(text):
    if not text:
        return None
    m = re.search(r"[\d,]+", str(text))
    return int(m.group(0).replace(",", "")) if m else None

def post_id_from_url(url):
    qs = parse_qs(urlparse(url).query)
    if qs.get("id"):
        return qs["id"][0]
    m = re.search(r"[?&]id=(\d+)", url)
    return m.group(1) if m else None

def build_canonical_url(post_id):
    if not post_id:
        raise InvalidPostIdentity("Cannot build URL without post_id")
    # Live request instrumentation (2026-08-17) confirmed that MLBPARK turns
    # this stable URL into a signed `sig=` URL via one required 302. Omitting
    # m=view still redirects, while persisting the short-lived signed URL would
    # make resume state brittle, so the stable canonical URL is intentional.
    return f"{BASE}?m=view&b=bullpen&id={post_id}"

def polite_sleep(delay, jitter=0.0):
    time.sleep(max(0.0, delay + random.uniform(0.0, jitter)))

def parse_post_date(created_at_str):
    """Extract date from created_at string.
    Actual format from div.text3: '2026-08-17 07:59' → '2026-08-17'
    Verified against live MLBPark HTML."""
    if not created_at_str:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", created_at_str)
    return m.group(1) if m else None


def seoul_today():
    """MLBPARK renders time-only search dates in Korea Standard Time."""
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).strftime("%Y-%m-%d")


def parse_search_result_date(value, today=None):
    """Normalize a date scoped to one actual search-result row."""
    value = clean(value)
    if not value:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}", value):
        return today or seoul_today()
    return None


def extract_search_result_rows(page, today=None):
    """Return only post links and dates that share the same search row.

    A page-wide ``span.date`` query also sees sidebar widgets (``2시간전``,
    ``2026.07월``), which previously contaminated range termination.
    """
    records = []
    for row in page.css("tbody tr"):
        href = row.css('a[href*="m=view"][href*="id="]::attr(href)').get()
        post_id = post_id_from_url(href or "")
        if not post_id:
            continue
        raw_date = clean(row.css("span.date::text").get())
        records.append({
            "post_id": post_id,
            "href": href,
            "date": parse_search_result_date(raw_date, today=today),
            "raw_date": raw_date,
        })
    return records


def extract_category_from_page_html(page_html):
    """Read MLBPARK's canonical board-head value from its page metadata.

    The visible layout has changed several times, but every current post page
    exposes ``head: '주식'`` in the dataLayer contentData payload.  We make the
    category decision from that page-level value rather than guessing from a
    title or search result.
    """
    if not page_html:
        return None
    match = re.search(r"[\"']head[\"']\s*:\s*[\"']([^\"']+)[\"']", str(page_html))
    return clean(match.group(1)) if match else None

# ==========================================
# DB Schema & State Machine
# ==========================================
def ensure_schema(conn):
    conn.executescript(
        """
        create table if not exists posts (
          post_id text primary key,
          url text not null,
          category text,
          title text,
          author text,
          created_at text,
          views integer,
          recommendations integer,
          reply_count_reported integer,
          collected_comment_count integer not null default 0,
          content_text text,
          content_html text,
          raw_html blob,
          raw_html_r2_key text,
          raw_html_storage_path text,
          matched_keywords text,
          first_seen_at text,
          last_fetched_at text
        );
        create table if not exists comments (
          post_id text not null,
          comment_id text not null,
          author text,
          created_at text,
          body_text text,
          parent_author text,
          raw_html_r2_key text,
          raw_html_storage_path text,
          raw_html text,
          primary key (post_id, comment_id)
        );
        create table if not exists discoveries (
          keyword text not null,
          search_url text not null,
          post_id text not null,
          discovered_at text not null,
          primary key (keyword, search_url, post_id)
        );
        create table if not exists post_queue (
          post_id text primary key,
          canonical_url text not null,
          status text not null,
          attempt_count integer not null default 0,
          claimed_at text,
          last_error text,
          restored_at_start integer not null default 0
        );
        create table if not exists fetch_log (
          url text not null,
          fetched_at text not null,
          status integer,
          error text
        );
        create table if not exists discovery_progress (
          keyword text primary key,
          last_page_scanned integer not null default 0,
          completed_at text,
          updated_at text not null
        );
        """
    )
    ensure_column(conn, "discovery_progress", "completed_at", "text")
    ensure_column(conn, "posts", "collected_comment_count", "integer not null default 0")
    ensure_column(conn, "post_queue", "restored_at_start", "integer not null default 0")
    conn.commit()


def ensure_column(conn, table, column, sql_type):
    columns = {row[1] for row in conn.execute(f"pragma table_info({table})")}
    if column not in columns:
        conn.execute(f"alter table {table} add column {column} {sql_type}")


def queue_row(conn, post_id):
    cursor = conn.execute(
        "SELECT post_id,canonical_url,status,attempt_count,claimed_at,last_error FROM post_queue WHERE post_id=?",
        (post_id,),
    )
    row = cursor.fetchone()
    return dict(zip([item[0] for item in cursor.description], row)) if row else None


def transition_queue(conn, post_id, target_status, error_msg=None, supabase=None, crawl_scope="default"):
    """
    State Transition Enforcement:
    discovered -> processing
    retry -> processing
    processing -> done
    processing -> retry
    processing -> failed
    processing -> skipped  (date range filter — terminal, no post row expected)
    processing(stale) -> retry

    Note on 'skipped': terminal state for posts outside --start-date/--end-date range.
    If you need to re-collect skipped posts, use a fresh DB. This is intentional for
    backfill scenarios where range boundaries are fixed per run.
    """
    cur = conn.execute("SELECT status FROM post_queue WHERE post_id = ?", (post_id,))
    row = cur.fetchone()
    if not row:
        raise InvariantViolation(f"Cannot transition missing post_id={post_id} to {target_status}")

    current_status = row[0]

    # Validation Rules
    valid_transitions = {
        'discovered': ['processing'],
        'retry': ['processing'],
        'processing': ['done', 'retry', 'failed', 'skipped'],
        'done': [],
        'failed': [],
        'skipped': [],  # terminal — see docstring
    }

    if target_status not in valid_transitions.get(current_status, []):
        raise InvariantViolation(f"Invalid state transition: {current_status} -> {target_status} for {post_id}")

    now = now_iso()
    if target_status == 'processing':
        conn.execute(
            "UPDATE post_queue SET status=?, claimed_at=?, last_error=NULL, restored_at_start=0 WHERE post_id=?",
            (target_status, now, post_id),
        )
    elif target_status in ('retry', 'failed', 'skipped'):
        conn.execute(
            "UPDATE post_queue SET status=?, claimed_at=NULL, last_error=?, attempt_count=attempt_count+1 WHERE post_id=?",
            (target_status, error_msg, post_id)
        )
    elif target_status == 'done':
        conn.execute(
            "UPDATE post_queue SET status=?, claimed_at=NULL, last_error=NULL, attempt_count=attempt_count+1 WHERE post_id=?",
            (target_status, post_id),
        )
    else:
        raise Bug(f"Unknown target_status: {target_status}")
    remote_row = queue_row(conn, post_id)
    if supabase:
        remote_row["crawl_scope"] = crawl_scope
        remote_row["updated_at"] = now
        supabase.upsert("mlbpark_python_post_queue", [remote_row], "crawl_scope,post_id")
    conn.commit()

# ==========================================
# Phase 1: Discovery
# ==========================================
def discover_phase(conn, keyword, max_pages, timeout, delay, start_date=None, end_date=None, jitter=0.0, retries=1, supabase=None, crawl_scope="default"):
    """Scan up to ``max_pages`` new absolute pages and checkpoint each page.

    ``max_pages`` is a per-run budget, not an absolute ceiling. This makes a
    run resume from page 900 instead of repeatedly scanning pages 895–899.
    """
    encoded = quote_plus(keyword)

    cur = conn.execute("SELECT last_page_scanned, completed_at FROM discovery_progress WHERE keyword = ?", (keyword,))
    row = cur.fetchone()
    if row and row[1]:
        print(f"Discovery already complete for keyword '{keyword}' at {row[1]}.")
        return
    start_page = row[0] + 1 if row else 0

    # ⑤ Keyword-level flag: once sort assumption breaks, stay disabled for this keyword.
    # Trade-off: if disabled, discovery runs all the way to max_pages (no early stop).
    date_sort_reliable = True

    for page_index in range(start_page, start_page + max_pages):
        offset = page_index * 30 + 1
        url = f"{BASE}?p={offset}&m=search&b=bullpen&query={encoded}&select=sct&user="

        try:
            page = fetch(url, timeout, retries=retries)
        except CrawlBlockedError:
            raise
        except TransientFetchError:
            raise

        records = extract_search_result_rows(page)
        parsed_dates = [record["date"] for record in records if record["date"]]

        # ⑤ Sort Assumption Sanity Check — keyword-level disable
        if len(parsed_dates) > 1:
            inversions = sum(1 for i in range(len(parsed_dates)-1) if parsed_dates[i] < parsed_dates[i+1])
            if inversions > len(parsed_dates) * 0.3:
                print(f"WARNING: Sort assumption violated on page {page_index} for keyword '{keyword}'. "
                      f"{inversions} inversions out of {len(parsed_dates)} items. "
                      f"Disabling date-based early stop for this keyword.")
                date_sort_reliable = False

        for record in records:
            post_id = record["post_id"]
            canonical = build_canonical_url(post_id)
            ts = now_iso()

            conn.execute(
                "INSERT INTO discoveries (keyword, search_url, post_id, discovered_at) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (keyword, url, post_id, ts)
            )

            conn.execute(
                """
                INSERT INTO post_queue (post_id, canonical_url, status)
                VALUES (?, ?, 'discovered')
                ON CONFLICT(post_id) DO NOTHING
                """,
                (post_id, canonical)
            )

        conn.execute(
            "INSERT INTO discovery_progress (keyword, last_page_scanned, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(keyword) DO UPDATE SET last_page_scanned = excluded.last_page_scanned, updated_at = excluded.updated_at",
            (keyword, page_index, now_iso())
        )
        if supabase:
            discovered_rows = [queue_row(conn, record["post_id"]) for record in records]
            for item in discovered_rows:
                item["crawl_scope"] = crawl_scope
                item["updated_at"] = now_iso()
            supabase.upsert("mlbpark_python_post_queue", discovered_rows, "crawl_scope,post_id")
            supabase.upsert("mlbpark_python_crawl_state", [{
                "crawl_scope": crawl_scope,
                "keyword": keyword,
                "next_page": page_index + 1,
                "completed_at": None,
                "updated_at": now_iso(),
            }], "crawl_scope,keyword")
        conn.commit()

        # ④⑤ Date Range Break — only if sort assumption still holds
        if date_sort_reliable and parsed_dates and start_date:
            oldest_on_page = min(parsed_dates)
            if oldest_on_page < start_date:
                completed_at = now_iso()
                conn.execute(
                    "UPDATE discovery_progress SET completed_at=?, updated_at=? WHERE keyword=?",
                    (completed_at, completed_at, keyword),
                )
                if supabase:
                    supabase.upsert("mlbpark_python_crawl_state", [{
                        "crawl_scope": crawl_scope,
                        "keyword": keyword,
                        "next_page": page_index + 1,
                        "completed_at": completed_at,
                        "updated_at": completed_at,
                    }], "crawl_scope,keyword")
                conn.commit()
                print(f"Early stop: oldest on page ({oldest_on_page}) < start_date ({start_date}). "
                      f"Stopping discovery for keyword '{keyword}'.")
                break

        polite_sleep(delay, jitter)


# ==========================================
# Fetch / Extract
# ==========================================
def fetch(url, timeout, retries=3, backoff=15.0):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            page = Fetcher.get(
                url,
                stealthy_headers=True,
                timeout=timeout,
            )
            status = getattr(page, "status", None)
            if status in (403, 429):
                headers = getattr(page, "headers", None) or {}
                retry_after = headers.get("Retry-After") or headers.get("retry-after")
                raise CrawlBlockedError(url, status, retry_after)
            if status and status >= 500:
                time.sleep(backoff * attempt)
                continue
            return page
        except CrawlBlockedError:
            raise
        except Exception as exc:
            last_exc = exc
            time.sleep(backoff * attempt)

    raise TransientFetchError(f"fetch failed after retries url={url} error={last_exc!r}")


def extract_post(page, url, keywords):
    requested_post_id = post_id_from_url(url)
    if not requested_post_id:
        raise InvalidPostIdentity(f"Cannot extract requested post_id from url: {url}")

    actual_url = getattr(page, 'url', url)
    actual_post_id = post_id_from_url(str(actual_url))
    # ① Fixed: None also counts as mismatch. Previously `if actual_post_id and ...`
    # skipped the most common dangerous case (redirect to login/error/deleted page
    # where id param is absent). Note: MLBPark preserves id= even for deleted posts,
    # so this primarily guards against login redirects and future site changes.
    if actual_post_id != requested_post_id:
        raise InvalidPostIdentity(
            f"Identity mismatch: requested={requested_post_id}, "
            f"actual={actual_post_id}, url={actual_url}"
        )

    try:
        title_area = page.css("div.titles")
        title_texts = [clean(x) for x in page.css("div.titles::text").get_all()]
        title = clean(" ".join([x for x in title_texts if x]))
        if not title:
            title = clean(page.css("title::text").get(default=""))

        content_html = page.css("#contentDetail").get(default="")
        if not content_html:
            raise UnexpectedPageShape("content_html is empty. Page might be deleted or structure changed.")

        if not isinstance(content_html, (str, bytes)):
            content_html = str(content_html)

        if "<" not in content_html and ">" not in content_html and "Selector" in content_html:
            raise UnexpectedPageShape(f"content_html extraction failed. Repr found instead of HTML.")

        if content_html:
            content_doc = lxml_html.fromstring(content_html)
            content_text = clean(" ".join(content_doc.xpath(".//text()")))
        else:
            content_text = ""

        # ``div.titles`` no longer contains the board head reliably.  MLBPARK
        # emits it in dataLayer as contentData.head (e.g. "주식").
        html = page.body if isinstance(page.body, str) else str(page.body)
        category = extract_category_from_page_html(html)
        author = clean(page.css("span.nick::text").get(default=""))

        meta_text = " ".join(page.css("div.text3 *::text, div.text3::text").get_all())
        created_match = re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", meta_text)
        created_at = created_match.group(0) if created_match else None

        text2 = clean(" ".join(page.css("div.text2 *::text").get_all()))
        views = ints(re.search(r"조회\s*([\d,]+)", text2 or "").group(1)) if re.search(r"조회\s*([\d,]+)", text2 or "") else None
        recommendations = ints(page.css("#likeCnt::text").get(default=""))
        reply_count = ints(page.css("#replyCnt::text").get(default=""))
        matched = [kw for kw in keywords if kw in f"{title or ''} {content_text or ''}"]
    except Exception as e:
        if isinstance(e, (UnexpectedPageShape, InvalidPostIdentity)):
            raise
        raise UnexpectedPageShape(f"Post parsing failed: {e!r}")

    return {
        "post_id": requested_post_id,
        "url": url,
        "category": category,
        "title": title,
        "author": author,
        "created_at": created_at,
        "views": views,
        "recommendations": recommendations,
        "reply_count_reported": reply_count,
        "content_text": content_text,
        "content_html": content_html,
        "raw_html": html,
        "matched_keywords": json.dumps(matched, ensure_ascii=False),
    }

def item_texts(element):
    return [t for t in element.xpath(".//text()")]

def extract_comments(page, post_id):
    rows = []
    try:
        for idx, node in enumerate(page.css("div.other_con, div.my_con").get_all(), start=1):
            html_str = node.html if hasattr(node, "html") else (node.get() if hasattr(node, "get") else str(node))
            if not isinstance(html_str, (str, bytes)):
                html_str = str(html_str)

            cid_match = re.search(r'id=["\']reply_([^"\']+)', html_str)
            comment_id = cid_match.group(1) if cid_match else str(idx)
            sub = lxml_html.fromstring(html_str)
            names = [clean(x) for x in sub.cssselect(".name") for x in item_texts(x) if clean(x)]
            dates = [clean(x) for x in sub.cssselect(".date") for x in item_texts(x) if clean(x)]
            body = clean(" ".join(x for el in sub.cssselect("span.re_txt") for x in item_texts(el)))
            if not body:
                continue
            rows.append(
                {
                    "post_id": post_id,
                    "comment_id": comment_id,
                    "author": names[0] if names else None,
                    "created_at": dates[0] if dates else None,
                    "body_text": body,
                    "parent_author": names[1] if len(names) > 1 else None,
                    "raw_html": html_str,
                }
            )
    except (AttributeError, ValueError) as e:
        raise UnexpectedPageShape(f"Comment parsing failed: {e!r}")
    return rows


# ==========================================
# External Sinks
# ==========================================
class SupabaseSink:
    def __init__(self, url, key):
        self.url = url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=minimal",
            }
        )

    def upsert(self, table, rows, on_conflict):
        if not rows:
            return
        endpoint = f"{self.url}/rest/v1/{table}?on_conflict={on_conflict}"
        resp = self.session.post(endpoint, data=json.dumps(rows, ensure_ascii=False).encode("utf-8"))
        if resp.status_code >= 300:
            raise RuntimeError(f"Supabase upsert {table} failed: {resp.status_code} {resp.text[:500]}")

    def select(self, table, query):
        resp = self.session.get(f"{self.url}/rest/v1/{table}?{query}")
        if resp.status_code >= 300:
            raise RuntimeError(f"Supabase select {table} failed: {resp.status_code} {resp.text[:500]}")
        return resp.json()

    def select_all(self, table, query="select=*", page_size=1000):
        rows = []
        start = 0
        while True:
            headers = {"Range": f"{start}-{start + page_size - 1}"}
            resp = self.session.get(f"{self.url}/rest/v1/{table}?{query}", headers=headers)
            if resp.status_code >= 300:
                raise RuntimeError(f"Supabase select {table} failed: {resp.status_code} {resp.text[:500]}")
            batch = resp.json()
            rows.extend(batch)
            if len(batch) < page_size:
                return rows
            start += page_size

    def insert(self, table, rows):
        if not rows:
            return
        resp = self.session.post(
            f"{self.url}/rest/v1/{table}",
            data=json.dumps(rows, ensure_ascii=False).encode("utf-8"),
            headers={"Prefer": "return=minimal"},
        )
        if resp.status_code >= 300:
            raise RuntimeError(f"Supabase insert {table} failed: {resp.status_code} {resp.text[:500]}")

    def upload_gzip(self, bucket, path, payload):
        if not payload:
            raise ValueError("Cannot archive empty raw HTML")
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        path = path.strip("/")
        endpoint = f"{self.url}/storage/v1/object/{quote(bucket, safe='')}/{quote(path, safe='/')}"
        response = self.session.put(endpoint, data=gzip.compress(payload), headers={
            # Supabase Storage validates this header against its MIME allowlist.
            # Parameters such as `charset=utf-8` cause a 415 InvalidMimeType.
            "Content-Type": "text/html",
            "Content-Encoding": "gzip",
            "x-upsert": "true",
        })
        if response.status_code >= 300:
            raise RuntimeError(f"Supabase Storage upload failed: {response.status_code} {response.text[:500]}")
        return path


def make_supabase_sink(args):
    url = args.supabase_url or os.environ.get("SUPABASE_URL")
    key = args.supabase_key or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    # ③ Neither specified → Supabase intentionally not used.
    if not url and not key:
        return None
    # At least one specified → user intends to use Supabase. Partial config = FATAL.
    missing = [k for k, v in [("SUPABASE_URL", url), ("SUPABASE_SERVICE_ROLE_KEY", key)] if not v]
    if missing:
        raise SystemExit(f"FATAL: Supabase partially configured, missing: {missing}")
    return SupabaseSink(url, key)


def make_supabase_storage_config(args, supabase):
    bucket = (args.supabase_storage_bucket or "").strip()
    if not bucket:
        return None
    if not supabase:
        raise SystemExit("FATAL: --supabase-storage-bucket requires Supabase credentials")
    return {"bucket": bucket, "prefix": args.supabase_storage_prefix.strip("/")}


def hydrate_durable_state(conn, supabase, keywords, crawl_scope):
    """Restore queue/cursor from Supabase before an ephemeral CI run starts."""
    if not supabase:
        return
    scope_filter = quote(crawl_scope, safe="")
    states = supabase.select_all(
        "mlbpark_python_crawl_state",
        f"select=keyword,next_page,completed_at,updated_at&crawl_scope=eq.{scope_filter}",
    )
    for state in states:
        if state["keyword"] not in keywords:
            continue
        conn.execute("INSERT INTO discovery_progress(keyword,last_page_scanned,completed_at,updated_at) VALUES(?,?,?,?) ON CONFLICT(keyword) DO UPDATE SET last_page_scanned=excluded.last_page_scanned,completed_at=excluded.completed_at,updated_at=excluded.updated_at", (state["keyword"], state["next_page"] - 1, state.get("completed_at"), state["updated_at"]))
    for row in supabase.select_all(
        "mlbpark_python_post_queue",
        f"select=post_id,canonical_url,status,attempt_count,claimed_at,last_error&crawl_scope=eq.{scope_filter}",
    ):
        conn.execute("INSERT INTO post_queue(post_id,canonical_url,status,attempt_count,claimed_at,last_error,restored_at_start) VALUES(?,?,?,?,?,?,1) ON CONFLICT(post_id) DO UPDATE SET canonical_url=excluded.canonical_url,status=excluded.status,attempt_count=excluded.attempt_count,claimed_at=excluded.claimed_at,last_error=excluded.last_error,restored_at_start=1", (row["post_id"], row["canonical_url"], row["status"], row["attempt_count"], row.get("claimed_at"), row.get("last_error")))
    conn.commit()


def persist_durable_state(conn, supabase, crawl_scope):
    """Final cursor checkpoint only; queue transitions are persisted inline."""
    if not supabase:
        return
    states = [{"crawl_scope": crawl_scope, "keyword": row[0], "next_page": row[1] + 1, "completed_at": row[2], "updated_at": row[3]} for row in conn.execute("SELECT keyword,last_page_scanned,completed_at,updated_at FROM discovery_progress")]
    supabase.upsert("mlbpark_python_crawl_state", states, "crawl_scope,keyword")


def record_fetch_failure(conn, supabase, url, status, error):
    row = {"url": url, "fetched_at": now_iso(), "status": status, "error": str(error)[:2000]}
    conn.execute(
        "INSERT INTO fetch_log(url,fetched_at,status,error) VALUES(?,?,?,?)",
        (row["url"], row["fetched_at"], row["status"], row["error"]),
    )
    conn.commit()
    if supabase:
        supabase.insert("mlbpark_fetch_log", [row])


def archive_post_raw_html(post, comments, supabase, storage):
    if not storage:
        return
    raw_html = post.get("raw_html")
    if not raw_html:
        raise UnexpectedPageShape(f"raw_html missing for post_id={post.get('post_id')}")
    prefix = storage["prefix"]
    key = "/".join(item for item in (prefix, "posts", f"{post['post_id']}.html.gz") if item)
    path = supabase.upload_gzip(storage["bucket"], key, raw_html)
    post["raw_html_storage_path"] = path
    for comment in comments:
        comment["raw_html_storage_path"] = path


def export_jsonl(conn, output_path):
    if not output_path:
        return
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp")
    cursor = conn.execute("SELECT * FROM posts ORDER BY created_at, post_id")
    columns = [description[0] for description in cursor.description]
    with temporary.open("w", encoding="utf-8") as handle:
        for row in cursor:
            post = dict(zip(columns, row))
            post.pop("raw_html", None)
            post.pop("content_html", None)
            try:
                post["matched_keywords"] = json.loads(post.get("matched_keywords") or "[]")
            except json.JSONDecodeError:
                post["matched_keywords"] = []
            comment_cursor = conn.execute("SELECT post_id, comment_id, author, created_at, body_text, parent_author, raw_html_storage_path FROM comments WHERE post_id=? ORDER BY comment_id", (post["post_id"],))
            names = [description[0] for description in comment_cursor.description]
            post["comments"] = [dict(zip(names, comment)) for comment in comment_cursor]
            handle.write(json.dumps(post, ensure_ascii=False, separators=(",", ":")) + "\n")
    temporary.replace(target)

def upload_post_to_external(post, comments, supabase):
    if supabase:
        try:
            clean_post = dict(post)
            clean_post.pop("raw_html", None)
            clean_post.pop("content_html", None)
            clean_post["matched_keywords"] = json.loads(clean_post.get("matched_keywords") or "[]")
            clean_post["last_fetched_at"] = now_iso()
            clean_post.setdefault("first_seen_at", clean_post["last_fetched_at"])
            supabase.upsert("mlbpark_posts", [clean_post], "post_id")

            if comments:
                clean_comments = []
                for row in comments:
                    out = dict(row)
                    out.pop("raw_html", None)
                    clean_comments.append(out)
                supabase.upsert("mlbpark_comments", clean_comments, "post_id,comment_id")
        except Exception as e:
            raise RuntimeError(f"Supabase failed: {e}") from e


# ==========================================
# Main Execution Phases
# ==========================================
def fetch_and_parse(url, keywords, timeout, retries=3, backoff=15.0):
    page = fetch(url, timeout, retries=retries, backoff=backoff)
    post = extract_post(page, url, keywords)

    comments = []
    try:
        comments = extract_comments(page, post["post_id"])
    except UnexpectedPageShape as e:
        print(f"Warning: Comment parse failure (partial success): {e}")
    post["collected_comment_count"] = len(comments)
    return page, post, comments

def local_commit(conn, post, comments):
    ts = now_iso()
    conn.execute(
        """
        insert into posts
        (post_id, url, category, title, author, created_at, views, recommendations,
         reply_count_reported, collected_comment_count, content_text, content_html, raw_html, raw_html_storage_path,
         matched_keywords, first_seen_at, last_fetched_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(post_id) do update set
          title=excluded.title, views=excluded.views, recommendations=excluded.recommendations,
          reply_count_reported=excluded.reply_count_reported,
          collected_comment_count=excluded.collected_comment_count, content_text=excluded.content_text,
          content_html=excluded.content_html, raw_html=excluded.raw_html,
          raw_html_storage_path=excluded.raw_html_storage_path, matched_keywords=excluded.matched_keywords,
          last_fetched_at=excluded.last_fetched_at
        """,
        (post["post_id"], post["url"], post["category"], post["title"], post["author"], post["created_at"],
         post["views"], post["recommendations"], post["reply_count_reported"], post.get("collected_comment_count", len(comments)), post["content_text"],
         post["content_html"], post["raw_html"], post.get("raw_html_storage_path"),
         post["matched_keywords"], ts, ts)
    )
    for row in comments:
        conn.execute(
            """
            insert or replace into comments
            (post_id, comment_id, author, created_at, body_text, parent_author, raw_html, raw_html_storage_path)
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (row["post_id"], row["comment_id"], row["author"], row["created_at"], row["body_text"], row["parent_author"], row["raw_html"], row.get("raw_html_storage_path"))
        )
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--jsonl")
    ap.add_argument("--keywords", nargs="+", required=True)
    ap.add_argument(
        "--crawl-scope",
        required=True,
        help="Stable dataset/run identity used to isolate durable queue and cursor state",
    )
    ap.add_argument("--max-search-pages", type=int, default=1, help="New search pages to scan per keyword in this run")
    ap.add_argument("--delay", type=float, default=1.5)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--limit-posts", type=int)
    ap.add_argument("--no-local-raw-html", action="store_true")

    # Range parameters
    ap.add_argument("--start-date", help="YYYY-MM-DD")
    ap.add_argument("--end-date", help="YYYY-MM-DD")
    ap.add_argument(
        "--required-category",
        default=None,
        help="Only persist posts whose MLBPARK page head matches this category. Use an empty value to disable.",
    )

    # Fetch tuning — exposed so workflow YAML can override defaults
    ap.add_argument("--jitter", type=float, default=0.0, help="Random extra delay seconds added to --delay")
    ap.add_argument("--retries", type=int, default=3, help="Max fetch retries per post")
    ap.add_argument("--backoff", type=float, default=15.0, help="Base backoff seconds between retries")

    # Supabase Database + Storage are the only operational external sinks.
    ap.add_argument("--supabase-url")
    ap.add_argument("--supabase-key")
    ap.add_argument("--supabase-storage-bucket")
    ap.add_argument("--supabase-storage-prefix", default="mlbpark/samsung")
    args = ap.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    supabase = make_supabase_sink(args)
    storage = make_supabase_storage_config(args, supabase)
    hydrate_durable_state(conn, supabase, args.keywords, args.crawl_scope)
    if args.no_local_raw_html and not storage:
        raise SystemExit("FATAL: --no-local-raw-html requires --supabase-storage-bucket")

    # 1. Discovery Phase
    for keyword in args.keywords:
        try:
            discover_phase(
                conn, keyword, args.max_search_pages, args.timeout, args.delay,
                args.start_date, args.end_date, jitter=args.jitter, retries=args.retries,
                supabase=supabase, crawl_scope=args.crawl_scope,
            )
        except CrawlBlockedError as exc:
            record_fetch_failure(conn, supabase, exc.url, exc.status, exc)
            print(f"Crawl stopped immediately: {exc}")
            persist_durable_state(conn, supabase, args.crawl_scope)
            conn.close()
            return
        except TransientFetchError as exc:
            print(f"Network error during discovery for keyword {keyword}: {exc}")
            continue
        except Exception as e:
            conn.rollback()
            raise SystemExit(f"FATAL discovery persistence/parsing error: {e!r}")

    # 2. Worker Phase
    stale_threshold = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=15)).isoformat()

    # Transition stale items back to retry
    cur = conn.execute(
        "SELECT post_id FROM post_queue WHERE status='processing' AND claimed_at < ?",
        (stale_threshold,)
    )
    stale_ids = [r[0] for r in cur.fetchall()]
    for sid in stale_ids:
        transition_queue(conn, sid, 'retry', 'stale_timeout', supabase=supabase, crawl_scope=args.crawl_scope)

    cur = conn.execute(
        "SELECT post_id, canonical_url FROM post_queue WHERE status IN ('discovered', 'retry')"
    )
    items = cur.fetchall()

    if args.limit_posts:
        items = items[:args.limit_posts]

    consecutive_network_errors = 0

    for post_id, canonical_url in items:
        transition_queue(conn, post_id, 'processing', supabase=supabase, crawl_scope=args.crawl_scope)

        try:
            # retries/backoff connected from argparse → fetch_and_parse → fetch
            page, post, comments = fetch_and_parse(
                canonical_url, args.keywords, args.timeout,
                retries=args.retries, backoff=args.backoff,
            )

            # ② Date range filtering — BEFORE local_commit() to preserve
            # done_without_post=0 invariant. skipped posts get no posts row.
            post_date = parse_post_date(post.get("created_at"))
            if post_date:
                if args.start_date and post_date < args.start_date:
                    transition_queue(conn, post_id, 'skipped', f'before start_date: {post_date}', supabase=supabase, crawl_scope=args.crawl_scope)
                    polite_sleep(args.delay, args.jitter)
                    continue
                if args.end_date and post_date > args.end_date:
                    transition_queue(conn, post_id, 'skipped', f'after end_date: {post_date}', supabase=supabase, crawl_scope=args.crawl_scope)
                    polite_sleep(args.delay, args.jitter)
                    continue

            if args.required_category and post.get("category") != args.required_category:
                actual_category = post.get("category") or "missing"
                transition_queue(
                    conn,
                    post_id,
                    "skipped",
                    f"category mismatch: expected {args.required_category}, got {actual_category}",
                    supabase=supabase,
                    crawl_scope=args.crawl_scope,
                )
                polite_sleep(args.delay, args.jitter)
                continue

            archive_post_raw_html(post, comments, supabase, storage)

            if args.no_local_raw_html:
                post["raw_html"] = None
                post["content_html"] = None
                for c in comments: c["raw_html"] = None

            local_commit(conn, post, comments)
            upload_post_to_external(post, comments, supabase)
            transition_queue(conn, post_id, 'done', supabase=supabase, crawl_scope=args.crawl_scope)

        except CrawlBlockedError as e:
            conn.rollback()
            record_fetch_failure(conn, supabase, e.url, e.status, e)
            transition_queue(conn, post_id, 'retry', str(e), supabase=supabase, crawl_scope=args.crawl_scope)
            print(f"Crawl stopped immediately: {e}")
            break
        except TransientFetchError as e:
            conn.rollback()
            record_fetch_failure(conn, supabase, canonical_url, None, e)
            transition_queue(conn, post_id, 'retry', str(e), supabase=supabase, crawl_scope=args.crawl_scope)
            consecutive_network_errors += 1
            if consecutive_network_errors >= 5:
                print("Circuit Breaker activated: 5 consecutive network errors. Escaping worker loop gracefully.")
                break
            continue
        except (UnexpectedPageShape, InvalidPostIdentity) as e:
            conn.rollback()
            record_fetch_failure(conn, supabase, canonical_url, None, e)
            transition_queue(conn, post_id, 'failed', str(e), supabase=supabase, crawl_scope=args.crawl_scope)
        except (InvariantViolation, Bug) as e:
            conn.rollback()
            raise SystemExit(f"FATAL: {e!r}")
        except Exception as e:
            conn.rollback()
            raise SystemExit(f"FATAL UNKNOWN BUG: {e!r}")

        consecutive_network_errors = 0
        polite_sleep(args.delay, args.jitter)

    # 3. Final checkpoint (transitions are already persisted immediately).
    persist_durable_state(conn, supabase, args.crawl_scope)
    export_jsonl(conn, args.jsonl)
    conn.close()

if __name__ == "__main__":
    main()
