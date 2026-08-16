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
from urllib.parse import quote_plus, urljoin, urlparse, parse_qs

import boto3
import requests
from lxml import html as lxml_html
from scrapling.fetchers import Fetcher

BASE = "https://mlbpark.donga.com/mp/b.php"


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
        create table if not exists fetch_log (
          url text not null,
          fetched_at text not null,
          status integer,
          error text
        );
        create table if not exists discovery_progress (
          keyword text primary key,
          last_page_scanned integer not null default 0,
          updated_at text not null
        );
        """
    )
    ensure_column(conn, "posts", "raw_html_r2_key", "text")
    ensure_column(conn, "posts", "raw_html_storage_path", "text")
    ensure_column(conn, "comments", "raw_html_r2_key", "text")
    ensure_column(conn, "comments", "raw_html_storage_path", "text")
    ensure_column(conn, "discoveries", "status", "text default 'discovered'")
    ensure_column(conn, "discoveries", "post_status", "text")
    ensure_column(conn, "discoveries", "comment_status", "text")
    ensure_column(conn, "discoveries", "claimed_at", "text")
    ensure_column(conn, "discoveries", "attempt_count", "integer default 0")
    ensure_column(conn, "discoveries", "last_error", "text")
    conn.commit()


def ensure_column(conn, table, column, sql_type):
    cols = {row[1] for row in conn.execute(f"pragma table_info({table})")}
    if column not in cols:
        conn.execute(f"alter table {table} add column {column} {sql_type}")


def polite_sleep(delay, jitter=0.0):
    time.sleep(max(0.0, delay + random.uniform(0.0, jitter)))


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
            if status in (403, 429) or (status and status >= 500):
                wait = backoff * attempt
                print(f"backoff status={status} attempt={attempt}/{retries} wait={wait:.1f}s url={url}")
                time.sleep(wait)
                continue
            return page
        except Exception as exc:
            last_exc = exc
            wait = backoff * attempt
            print(f"backoff error={exc!r} attempt={attempt}/{retries} wait={wait:.1f}s url={url}")
            time.sleep(wait)
    if last_exc:
        raise RuntimeError(f"fetch failed after retries url={url} error={last_exc!r}") from last_exc
    raise RuntimeError(f"fetch failed after retries url={url}")


def date_from_post_id(post_id):
    if not post_id or len(post_id) < 8:
        return None
    try:
        return dt.date.fromisoformat(f"{post_id[:4]}-{post_id[4:6]}-{post_id[6:8]}")
    except ValueError:
        return None


def discover(conn, supabase, keyword, max_pages, timeout, delay, jitter=0.0, start_date=None, stop_after_older_pages=3):
    encoded = quote_plus(keyword)
    older_page_streak = 0
    
    cur = conn.execute("SELECT last_page_scanned FROM discovery_progress WHERE keyword = ?", (keyword,))
    row = cur.fetchone()
    last_page = row[0] if row else 0
    start_page = max(0, last_page - 5)
    
    for page_index in range(start_page, max_pages):
        offset = page_index * 30 + 1
        url = f"{BASE}?p={offset}&m=search&b=bullpen&query={encoded}&select=sct&user="
        page = fetch(url, timeout)
        links = page.css("td.t_left a::attr(href)").get_all()
        page_dates = []
        
        discovery_rows = []
        for href in links:
            full = urljoin(BASE, href)
            post_id = post_id_from_url(full)
            if post_id:
                post_date = date_from_post_id(post_id)
                if post_date:
                    page_dates.append(post_date)
                
                conn.execute(
                    """
                    INSERT INTO discoveries 
                    (keyword, search_url, post_id, discovered_at, status, attempt_count) 
                    VALUES (?, ?, ?, ?, 'discovered', 0)
                    ON CONFLICT(keyword, search_url, post_id) DO NOTHING
                    """,
                    (keyword, url, post_id, now_iso())
                )
                discovery_rows.append({
                    "keyword": keyword,
                    "search_url": url,
                    "post_id": post_id,
                    "discovered_at": now_iso()
                })
                
        if supabase and discovery_rows:
            try:
                supabase.upsert("mlbpark_discoveries", discovery_rows, "keyword,search_url,post_id")
            except Exception as e:
                print(f"Warning: Supabase discovery log failed: {e}")
                
        conn.execute(
            "INSERT INTO discovery_progress (keyword, last_page_scanned, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(keyword) DO UPDATE SET last_page_scanned = excluded.last_page_scanned, updated_at = excluded.updated_at",
            (keyword, page_index, now_iso())
        )
        conn.commit()
        print(f"Discovery: keyword={keyword} page={page_index} offset={offset} found={len(discovery_rows)}")
        
        if start_date and page_dates and max(page_dates) < start_date:
            older_page_streak += 1
            if older_page_streak >= stop_after_older_pages:
                print(f"stop discovery keyword={keyword} offset={offset} reason=older_than_start_date")
                break
        else:
            older_page_streak = 0
        polite_sleep(delay, jitter)


def extract_post(page, url, keywords):
    post_id = post_id_from_url(url)
    title_area = page.css("div.titles")
    category = clean(title_area.css("a::text").get(default="")) if title_area else None
    title_texts = [clean(x) for x in page.css("div.titles::text").get_all()]
    title = clean(" ".join([x for x in title_texts if x]))
    if not title:
        title = clean(page.css("title::text").get(default=""))
    author = clean(page.css("span.nick::text").get(default=""))
    meta_text = " ".join(page.css("div.text3 *::text, div.text3::text").get_all())
    created_match = re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", meta_text)
    if not created_match:
        created_match = re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", page.body or "")
    created_at = created_match.group(0) if created_match else None
    text2 = clean(" ".join(page.css("div.text2 *::text").get_all()))
    views = None
    view_match = re.search(r"조회\s*([\d,]+)", text2 or "")
    if view_match:
        views = ints(view_match.group(1))
    recommendations = ints(page.css("#likeCnt::text").get(default=""))
    reply_count = ints(page.css("#replyCnt::text").get(default=""))
    content_html = page.css("#contentDetail").get(default="")
    if not isinstance(content_html, (str, bytes)):
        content_html = str(content_html)
    if content_html:
        content_doc = lxml_html.fromstring(content_html)
        content_text = clean(" ".join(content_doc.xpath(".//text()")))
    else:
        content_text = ""
    html = page.body
    if not isinstance(html, (str, bytes)):
        html = str(html)
    matched = [kw for kw in keywords if kw in f"{title or ''} {content_text or ''}"]
    return {
        "post_id": post_id,
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


def parse_date(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def in_date_range(post, start_date, end_date):
    created = parse_date(post.get("created_at"))
    if not created:
        return True
    return start_date <= created <= end_date


def extract_comments(page, post_id):
    rows = []
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
    return rows


def item_texts(element):
    return [t for t in element.xpath(".//text()")]


class R2Sink:
    def __init__(self, bucket, endpoint_url, access_key_id, secret_access_key, prefix):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",
        )

    def key(self, *parts):
        joined = "/".join(str(p).strip("/") for p in parts if str(p).strip("/"))
        return f"{self.prefix}/{joined}" if self.prefix else joined

    def put_gzip(self, key, payload, content_type):
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        body = gzip.compress(payload)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
            ContentEncoding="gzip",
        )
        return key


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
        rows = dedupe_rows(rows, on_conflict.split(","))
        endpoint = f"{self.url}/rest/v1/{table}?on_conflict={on_conflict}"
        resp = self.session.post(endpoint, data=json.dumps(rows, ensure_ascii=False).encode("utf-8"))
        if resp.status_code >= 300:
            raise RuntimeError(f"Supabase upsert {table} failed: {resp.status_code} {resp.text[:500]}")

    def insert(self, table, rows):
        if not rows:
            return
        endpoint = f"{self.url}/rest/v1/{table}"
        resp = self.session.post(endpoint, data=json.dumps(rows, ensure_ascii=False).encode("utf-8"))
        if resp.status_code >= 300:
            raise RuntimeError(f"Supabase insert {table} failed: {resp.status_code} {resp.text[:500]}")


class SupabaseStorageSink:
    def __init__(self, url, key, bucket, prefix):
        self.url = url.rstrip("/")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "apikey": key,
                "Authorization": f"Bearer {key}",
            }
        )

    def path(self, *parts):
        joined = "/".join(str(p).strip("/") for p in parts if str(p).strip("/"))
        return f"{self.prefix}/{joined}" if self.prefix else joined

    def put_gzip(self, path, payload, content_type):
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        body = gzip.compress(payload)
        endpoint = f"{self.url}/storage/v1/object/{self.bucket}/{path}"
        headers = {
            "Content-Type": content_type.split(";", 1)[0],
            "Content-Encoding": "gzip",
            "x-upsert": "true",
        }
        resp = self.session.post(endpoint, data=body, headers=headers)
        if resp.status_code >= 300:
            raise RuntimeError(f"Supabase Storage upload failed: {resp.status_code} {resp.text[:500]}")
        return path


def make_r2_sink(args):
    if not args.r2_bucket:
        return None
    endpoint = args.r2_endpoint_url or os.environ.get("R2_ENDPOINT_URL")
    access_key = args.r2_access_key_id or os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = args.r2_secret_access_key or os.environ.get("R2_SECRET_ACCESS_KEY")
    if not endpoint or not access_key or not secret_key:
        raise SystemExit("R2 requires --r2-endpoint-url/--r2-access-key-id/--r2-secret-access-key or matching env vars")
    return R2Sink(args.r2_bucket, endpoint, access_key, secret_key, args.r2_prefix)


def dedupe_rows(rows, key_fields):
    seen = set()
    out = []
    for row in rows:
        key = tuple(row.get(field) for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def make_supabase_sink(args):
    url = args.supabase_url or os.environ.get("SUPABASE_URL")
    key = args.supabase_key or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url and not key:
        return None
    if not url or not key:
        raise SystemExit("Supabase remote writes require --supabase-url/--supabase-key or SUPABASE_URL plus SUPABASE_SERVICE_ROLE_KEY")
    return SupabaseSink(url, key)


def make_supabase_storage_sink(args):
    if not args.supabase_storage_bucket:
        return None
    url = args.supabase_url or os.environ.get("SUPABASE_URL")
    key = args.supabase_key or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise SystemExit("Supabase Storage requires --supabase-url/--supabase-key or SUPABASE_URL plus SUPABASE_SERVICE_ROLE_KEY")
    return SupabaseStorageSink(url, key, args.supabase_storage_bucket, args.supabase_storage_prefix)


def strip_large_local_fields(post, comments):
    clean_post = dict(post)
    clean_post.pop("raw_html", None)
    clean_post.pop("content_html", None)
    clean_comments = []
    for row in comments:
        out = dict(row)
        out.pop("raw_html", None)
        clean_comments.append(out)
    return clean_post, clean_comments


def upload_raw_to_r2(r2, post):
    if not r2 or not post.get("raw_html"):
        return
    post_id = post["post_id"]
    post_key = r2.key("posts", f"{post_id}.html.gz")
    post["raw_html_r2_key"] = r2.put_gzip(post_key, post["raw_html"], "text/html; charset=utf-8")

def upload_comments_raw_to_r2(r2, post_id, comments):
    if not r2 or not comments:
        return
    for row in comments:
        if row.get("raw_html"):
            comment_key = r2.key("comments", post_id, f"{row['comment_id']}.html.gz")
            row["raw_html_r2_key"] = r2.put_gzip(comment_key, row["raw_html"], "text/html; charset=utf-8")


def upload_raw_to_supabase_storage(storage, post):
    if not storage or not post.get("raw_html"):
        return
    post_id = post["post_id"]
    post_path = storage.path("posts", f"{post_id}.html.gz")
    post["raw_html_storage_path"] = storage.put_gzip(post_path, post["raw_html"], "text/html; charset=utf-8")

def upload_comments_raw_to_supabase_storage(storage, post_id, comments):
    if not storage or not comments:
        return
    for row in comments:
        if row.get("raw_html"):
            comment_path = storage.path("comments", post_id, f"{row['comment_id']}.html.gz")
            row["raw_html_storage_path"] = storage.put_gzip(comment_path, row["raw_html"], "text/html; charset=utf-8")


def upsert_post(conn, post):
    ts = now_iso()
    conn.execute(
        """
        insert into posts
        (post_id, url, category, title, author, created_at, views, recommendations,
         reply_count_reported, content_text, content_html, raw_html, raw_html_r2_key, raw_html_storage_path,
         matched_keywords, first_seen_at, last_fetched_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(post_id) do update set
          url=excluded.url,
          category=excluded.category,
          title=excluded.title,
          author=excluded.author,
          created_at=excluded.created_at,
          views=excluded.views,
          recommendations=excluded.recommendations,
          reply_count_reported=excluded.reply_count_reported,
          content_text=excluded.content_text,
          content_html=excluded.content_html,
          raw_html=excluded.raw_html,
          raw_html_r2_key=excluded.raw_html_r2_key,
          raw_html_storage_path=excluded.raw_html_storage_path,
          matched_keywords=excluded.matched_keywords,
          last_fetched_at=excluded.last_fetched_at
        """,
        (
            post["post_id"],
            post["url"],
            post["category"],
            post["title"],
            post["author"],
            post["created_at"],
            post["views"],
            post["recommendations"],
            post["reply_count_reported"],
            post["content_text"],
            post["content_html"],
            post["raw_html"],
            post.get("raw_html_r2_key"),
            post.get("raw_html_storage_path"),
            post["matched_keywords"],
            ts,
            ts,
        ),
    )

def upsert_comments(conn, comments):
    for row in comments:
        conn.execute(
            """
            insert or replace into comments
            (post_id, comment_id, author, created_at, body_text, parent_author, raw_html_r2_key, raw_html_storage_path, raw_html)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["post_id"],
                row["comment_id"],
                row["author"],
                row["created_at"],
                row["body_text"],
                row["parent_author"],
                row.get("raw_html_r2_key"),
                row.get("raw_html_storage_path"),
                row["raw_html"],
            ),
        )


def upsert_supabase_post(supabase, post):
    if not supabase:
        return
    clean_post, _ = strip_large_local_fields(post, [])
    clean_post["matched_keywords"] = json.loads(clean_post.get("matched_keywords") or "[]")
    clean_post["last_fetched_at"] = now_iso()
    clean_post.setdefault("first_seen_at", clean_post["last_fetched_at"])
    supabase.upsert("mlbpark_posts", [clean_post], "post_id")

def upsert_supabase_comments(supabase, comments):
    if not supabase or not comments:
        return
    _, clean_comments = strip_large_local_fields({}, comments)
    supabase.upsert("mlbpark_comments", clean_comments, "post_id,comment_id")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--jsonl")
    ap.add_argument("--start-date", default="2024-01-01")
    ap.add_argument("--end-date", default=dt.date.today().isoformat())
    ap.add_argument("--keywords", nargs="+", required=True)
    ap.add_argument("--max-search-pages", type=int, default=1)
    ap.add_argument("--delay", type=float, default=1.5)
    ap.add_argument("--jitter", type=float, default=0.0)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--backoff", type=float, default=15.0)
    ap.add_argument("--stop-after-older-pages", type=int, default=3)
    ap.add_argument("--limit-posts", type=int)
    ap.add_argument("--no-local-raw-html", action="store_true")
    ap.add_argument("--r2-bucket")
    ap.add_argument("--r2-prefix", default="mlbpark/samsung")
    ap.add_argument("--r2-endpoint-url")
    ap.add_argument("--r2-access-key-id")
    ap.add_argument("--r2-secret-access-key")
    ap.add_argument("--supabase-url")
    ap.add_argument("--supabase-key")
    ap.add_argument("--supabase-storage-bucket")
    ap.add_argument("--supabase-storage-prefix", default="mlbpark/samsung")
    args = ap.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if args.jsonl:
        Path(args.jsonl).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    ensure_schema(conn)
    r2 = make_r2_sink(args)
    supabase = make_supabase_sink(args)
    supabase_storage = make_supabase_storage_sink(args)
    start_date = dt.date.fromisoformat(args.start_date)
    end_date = dt.date.fromisoformat(args.end_date)

    # 1. 탐색 페이즈 (Discovery Queueing)
    for keyword in args.keywords:
        try:
            discover(
                conn,
                supabase,
                keyword,
                args.max_search_pages,
                args.timeout,
                args.delay,
                jitter=args.jitter,
                start_date=start_date,
                stop_after_older_pages=args.stop_after_older_pages,
            )
        except RuntimeError as exc:
            print(f"Network error during discovery for keyword {keyword}: {exc}")
            continue

    # 2. 작업 큐 가져오기 (Worker Phase)
    # status='discovered' 이거나 처리된 지 15분 지난 'processing' (Stale)
    stale_threshold = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=15)).isoformat()
    cur = conn.execute(
        """
        SELECT post_id, MAX(search_url), GROUP_CONCAT(keyword)
        FROM discoveries
        WHERE status = 'discovered' OR (status = 'processing' AND claimed_at < ?)
        GROUP BY post_id
        """,
        (stale_threshold,)
    )
    items = []
    for row in cur.fetchall():
        post_id_val = row[0]
        post_url = f"{BASE}?m=view&b=bullpen&id={post_id_val}"
        items.append((post_id_val, {"url": post_url, "keywords": set(row[2].split(","))}))
        
    if args.limit_posts:
        items = items[: args.limit_posts]

    jsonl_f = open(args.jsonl, "a", encoding="utf-8") if args.jsonl else None
    
    stats = {
        "discovered": len(items),
        "fetched": 0,
        "post_parsed": 0,
        "post_saved": 0,
        "comment_parsed": 0,
        "comment_saved": 0,
        "comment_failed": 0,
    }
    
    consecutive_network_errors = 0

    start_time = time.time()
    try:
        for index, (post_id, info) in enumerate(items, start=1):
            if time.time() - start_time > 19800:  # 5.5 hours = 5.5 * 3600 = 19800 seconds
                print("Graceful Shutdown: 5.5 hours elapsed. Checkpointing and exiting cleanly.")
                break
                
            conn.execute(
                "UPDATE discoveries SET status = 'processing', claimed_at = ? WHERE post_id = ?",
                (now_iso(), post_id)
            )
            conn.commit()

            try:
                # 1. FETCH & POST PARSE
                try:
                    page = fetch(info["url"], args.timeout, retries=args.retries, backoff=args.backoff)
                    stats["fetched"] += 1
                    consecutive_network_errors = 0
                except RuntimeError as exc:
                    conn.rollback()
                    print(f"Network error on {info['url']}: {exc}")
                    consecutive_network_errors += 1
                    
                    conn.execute(
                        "UPDATE discoveries SET attempt_count = attempt_count + 1, last_error = ? WHERE post_id = ?",
                        (str(exc), post_id)
                    )
                    conn.execute(
                        "UPDATE discoveries SET status = CASE WHEN attempt_count >= 3 THEN 'failed' ELSE 'discovered' END WHERE post_id = ?",
                        (post_id,)
                    )
                    conn.commit()

                    if consecutive_network_errors >= 5:
                        raise SystemExit(f"Circuit Breaker triggered: 5 consecutive network errors. Exiting.")
                    polite_sleep(args.delay, args.jitter)
                    continue

                post = extract_post(page, info["url"], args.keywords)
                stats["post_parsed"] += 1
                
                if not in_date_range(post, start_date, end_date):
                    print(f"skip post_id={post_id} created_at={post.get('created_at')}")
                    conn.execute(
                        "UPDATE discoveries SET status = 'done', post_status = 'skipped_date', attempt_count = attempt_count + 1 WHERE post_id = ?",
                        (post_id,)
                    )
                    conn.commit()
                    polite_sleep(args.delay, args.jitter)
                    continue
                    
                post["matched_keywords"] = json.dumps(sorted(info["keywords"]), ensure_ascii=False)

                # 2. SAVE POST FIRST (SQLite first, then Cloud)
                if args.no_local_raw_html:
                    post["raw_html"] = None
                    post["content_html"] = None
                    
                upsert_post(conn, post)
                upload_raw_to_r2(r2, post)
                upload_raw_to_supabase_storage(supabase_storage, post)
                upsert_supabase_post(supabase, post)
                
                stats["post_saved"] += 1
                
                # 3. PARSE & SAVE COMMENTS
                comments = []
                comment_status = 'done'
                try:
                    comments = extract_comments(page, post_id)
                    stats["comment_parsed"] += len(comments)
                    
                    if args.no_local_raw_html:
                        for row in comments:
                            row["raw_html"] = None
                    
                    if comments:
                        upsert_comments(conn, comments)
                        upload_comments_raw_to_r2(r2, post_id, comments)
                        upload_comments_raw_to_supabase_storage(supabase_storage, post_id, comments)
                        upsert_supabase_comments(supabase, comments)
                        stats["comment_saved"] += len(comments)
                except Exception as exc:
                    stats["comment_failed"] += 1
                    comment_status = 'failed'
                    import traceback
                    traceback.print_exc()
                    print(f"Warning: Failed to parse/save comments for post_id={post_id} (Post was saved). Error: {exc!r}")

                # 4. LOG & JSONL
                log_row = {"url": info["url"], "fetched_at": now_iso(), "status": page.status, "error": None}
                if supabase:
                    try:
                        supabase.insert("mlbpark_fetch_log", [log_row])
                    except Exception:
                        pass
                conn.execute(
                    "insert into fetch_log values (?, ?, ?, ?)",
                    (log_row["url"], log_row["fetched_at"], log_row["status"], log_row["error"]),
                )
                
                # 5. COMMIT DB & QUEUE STATUS (Strict Transaction Ordering)
                conn.execute(
                    "UPDATE discoveries SET status = 'done', post_status = 'done', comment_status = ?, attempt_count = attempt_count + 1 WHERE post_id = ?",
                    (comment_status, post_id)
                )
                conn.commit()

                if jsonl_f:
                    out = dict(post)
                    out["comments"] = comments
                    jsonl_f.write(json.dumps(out, ensure_ascii=False) + "\n")
                    jsonl_f.flush()

                print(f"[{index}/{len(items)}] saved post_id={post_id} comments={len(comments)} title={post['title']}")

                # 6. EARLY VALIDATION & CHAOS TEST
                if stats["post_saved"] == 5 and os.environ.get("CHAOS_TEST"):
                    import os as _os
                    print("CHAOS TEST: 프로세스가 강제 종료(SIGKILL)되었습니다!")
                    _os._exit(1)

                if stats["post_saved"] == 10 and not os.environ.get("CHAOS_TEST"):
                    cursor = conn.execute("SELECT COUNT(*) FROM posts")
                    db_count = cursor.fetchone()[0]
                    print(f"Early Validation: {stats['post_saved']} posts saved in this run, DB currently has {db_count} posts total.")
                    if db_count == 0:
                        raise SystemExit(f"Validation Failed! 10 posts processed but DB count is 0. Exiting.")

            except (TypeError, ValueError, AttributeError) as exc:
                conn.rollback()
                import traceback
                traceback.print_exc()
                print(f"Parse Error for post_id={post_id}. Error: {exc!r}")
                conn.execute(
                    "UPDATE discoveries SET attempt_count = attempt_count + 1, last_error = ? WHERE post_id = ?",
                    (f"Parse Error: {exc!r}", post_id)
                )
                conn.execute(
                    "UPDATE discoveries SET status = CASE WHEN attempt_count >= 3 THEN 'failed' ELSE 'discovered' END WHERE post_id = ?",
                    (post_id,)
                )
                conn.commit()
            except Exception as exc:
                conn.rollback()
                import traceback
                traceback.print_exc()
                print(f"Unexpected error processing post_id={post_id}: {exc!r}")
                conn.execute(
                    "UPDATE discoveries SET attempt_count = attempt_count + 1, last_error = ? WHERE post_id = ?",
                    (f"General Error: {exc!r}", post_id)
                )
                conn.execute(
                    "UPDATE discoveries SET status = CASE WHEN attempt_count >= 3 THEN 'failed' ELSE 'discovered' END WHERE post_id = ?",
                    (post_id,)
                )
                conn.commit()
            
            polite_sleep(args.delay, args.jitter)

    finally:
        print(f"--- Final Stats ---")
        for k, v in stats.items():
            print(f"{k}: {v}")
        if jsonl_f:
            jsonl_f.close()
        conn.close()


if __name__ == "__main__":
    main()
