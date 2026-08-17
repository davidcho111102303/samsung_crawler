import gzip
import json
import sqlite3
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.append(str(Path(__file__).parent.parent / "scripts"))
import mlbpark_scrapling_collect as crawler
from mlbpark_scrapling_collect import UnexpectedPageShape
from scrapling import Adaptor

DB_PATH = "test_mlbpark.sqlite"

@pytest.fixture
def clean_db():
    if Path(DB_PATH).exists():
        Path(DB_PATH).unlink()
    conn = sqlite3.connect(DB_PATH)
    crawler.ensure_schema(conn)
    yield conn
    conn.close()
    if Path(DB_PATH).exists():
        Path(DB_PATH).unlink()

def insert_post_queue(conn, post_id, status='discovered', attempt=0):
    conn.execute(
        "INSERT INTO post_queue (post_id, canonical_url, status, attempt_count) VALUES (?, ?, ?, ?)",
        (post_id, crawler.build_canonical_url(post_id), status, attempt)
    )
    conn.commit()

class DummyPage:
    def __init__(self, html, status=200):
        self.html = html
        self.body = html
        self.status = status

    def css(self, selector):
        mock = MagicMock()
        mock.get.return_value = "dummy"
        mock.get_all.return_value = ["dummy"]
        return mock

# ==========================================
# T3: 댓글 파싱 에러 (UnexpectedPageShape) -> 부분 성공
# ==========================================
def test_T3_comment_parse_failure_downgrade(clean_db):
    insert_post_queue(clean_db, "12345678")

    with patch('mlbpark_scrapling_collect.fetch') as mock_fetch:
        mock_fetch.return_value = DummyPage("<html><body>test</body></html>")

        with patch('mlbpark_scrapling_collect.extract_comments') as mock_comments:
            # New architecture converts AttributeError to UnexpectedPageShape,
            # so since we are mocking the function directly, we simulate the exception it would raise.
            mock_comments.side_effect = UnexpectedPageShape("DOM Changed")

            with patch('sys.argv', ['test', '--db', DB_PATH, '--keywords', 'test', '--crawl-scope', 'test', '--limit-posts', '1']):
                crawler.main()

    cur = clean_db.execute("SELECT status FROM post_queue WHERE post_id='12345678'")
    row = cur.fetchone()
    assert row[0] == 'done', "Queue should be done (partial success)"

    cur = clean_db.execute("SELECT COUNT(*) FROM posts WHERE post_id='12345678'")
    assert cur.fetchone()[0] == 1, "Post should be saved"

    cur = clean_db.execute("SELECT COUNT(*) FROM comments WHERE post_id='12345678'")
    assert cur.fetchone()[0] == 0, "Comments should be 0 (rollback)"

# ==========================================
# T6: InvariantViolation (내부 논리 오류) -> Fatal
# ==========================================
def test_T6_invariant_violation_fatal(clean_db):
    insert_post_queue(clean_db, "12345678")

    with patch('mlbpark_scrapling_collect.fetch') as mock_fetch:
        mock_fetch.return_value = DummyPage("<html><body>test</body></html>")

        # 내부 논리 오류 모의: build_canonical_url이 InvalidPostIdentity 던짐
        with patch('mlbpark_scrapling_collect.extract_post', side_effect=crawler.Bug("Some python bug")):
            with patch('sys.argv', ['test', '--db', DB_PATH, '--keywords', 'test', '--crawl-scope', 'test', '--limit-posts', '1']):
                with pytest.raises(SystemExit):
                    crawler.main()

    cur = clean_db.execute("SELECT status FROM post_queue WHERE post_id='12345678'")
    assert cur.fetchone()[0] == 'processing', "Should remain processing after Fatal abort (Rollback)"

# ==========================================
# T8, T9: 동기화(Sync) 부분 실패 및 상태 독립
# ==========================================
def test_T8_T9_sync_partial_failure(clean_db):
    insert_post_queue(clean_db, "12345678")
    sink = MagicMock()
    sink.select_all.return_value = []

    with patch('mlbpark_scrapling_collect.fetch') as mock_fetch:
        mock_fetch.return_value = DummyPage("<html><body>test</body></html>")

        # T8: Supabase 에러 발생
        with patch('mlbpark_scrapling_collect.make_supabase_sink', return_value=sink), patch('mlbpark_scrapling_collect.upload_post_to_external', side_effect=RuntimeError("Supa Failed")), patch('mlbpark_scrapling_collect.hydrate_durable_state'), patch('mlbpark_scrapling_collect.persist_durable_state'):
            with patch('sys.argv', ['test', '--db', DB_PATH, '--keywords', 'test', '--crawl-scope', 'test', '--limit-posts', '1', '--supabase-url', 'http://a', '--supabase-key', 'k']):
                with pytest.raises(SystemExit, match="Supa Failed"):
                    crawler.main()

    cur = clean_db.execute("SELECT status FROM post_queue WHERE post_id='12345678'")
    assert cur.fetchone()[0] == 'processing', "A killed run must leave the item claim recoverable"

# ==========================================
# T11: Idempotency (A. Discovery, B. Storage)
# ==========================================
def test_T11A_discovery_idempotency(clean_db):
    """T11-A: 탐색 멱등성. 기존 done 상태가 discovered로 덮어씌워지지 않아야 함."""
    insert_post_queue(clean_db, "12345678", status='done')

    # Run discovery phase over same post
    with patch('mlbpark_scrapling_collect.fetch') as mock_fetch:
        page = DummyPage("<html><body>test</body></html>")
        page.css = MagicMock()
        page.css().get_all.return_value = ["/mp/b.php?m=view&id=12345678"]
        mock_fetch.return_value = page

        crawler.discover_phase(clean_db, "test", 1, 10, 0)

    cur = clean_db.execute("SELECT status FROM post_queue WHERE post_id='12345678'")
    assert cur.fetchone()[0] == 'done', "Queue status must not be reset to discovered"

def test_T11B_storage_idempotency(clean_db):
    """T11-B: 스토리지 멱등성. 두 번 process 해도 에러 안 나고 row 개수 유지."""
    post = {
        "post_id": "123", "url": "a", "category": "b", "title": "c", "author": "d",
        "created_at": "e", "views": 1, "recommendations": 1, "reply_count_reported": 1,
        "content_text": "f", "content_html": "g", "raw_html": "h", "matched_keywords": "[]"
    }
    comments = [{"post_id": "123", "comment_id": "1", "author": "a", "created_at": "b", "body_text": "c", "parent_author": "d", "raw_html": "e"}]

    # 1st commit
    crawler.local_commit(clean_db, post, comments)
    # 2nd commit
    crawler.local_commit(clean_db, post, comments)

    cur = clean_db.execute("SELECT COUNT(*) FROM posts WHERE post_id='123'")
    assert cur.fetchone()[0] == 1, "Should upsert, not duplicate"


def test_jsonl_export_is_structured_and_excludes_raw_html(clean_db, tmp_path):
    post = {"post_id": "123", "url": "a", "category": "주식", "title": "삼성전자", "author": "w", "created_at": "2026-08-17 00:00", "views": 1, "recommendations": 1, "reply_count_reported": 1, "content_text": "본문", "content_html": "<p>본문</p>", "raw_html": "<html>raw</html>", "matched_keywords": '["삼성전자"]'}
    comments = [{"post_id": "123", "comment_id": "1", "author": "r", "created_at": "now", "body_text": "댓글", "parent_author": None, "raw_html": "<div>raw</div>"}]
    crawler.local_commit(clean_db, post, comments)
    output = tmp_path / "posts.jsonl"
    crawler.export_jsonl(clean_db, output)
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["matched_keywords"] == ["삼성전자"]
    assert rows[0]["comments"][0]["body_text"] == "댓글"
    assert "raw_html" not in rows[0]


def test_raw_html_storage_archives_page_once_and_gzips():
    sink = crawler.SupabaseSink("https://example.supabase.co", "key")
    response = MagicMock(status_code=200, text="")
    sink.session.put = MagicMock(return_value=response)
    post = {"post_id": "123", "raw_html": "<html>source</html>"}
    comments = [{"post_id": "123", "comment_id": "1"}]
    crawler.archive_post_raw_html(post, comments, sink, {"bucket": "raw", "prefix": "mlbpark/samsung"})
    _, kwargs = sink.session.put.call_args
    assert gzip.decompress(kwargs["data"]) == b"<html>source</html>"
    assert kwargs["headers"]["Content-Type"] == "text/html"
    assert post["raw_html_storage_path"] == "mlbpark/samsung/posts/123.html.gz"
    assert comments[0]["raw_html_storage_path"] == post["raw_html_storage_path"]


def test_local_commit_persists_raw_html_storage_path(clean_db):
    post = {
        "post_id": "123", "url": "a", "category": "주식", "title": "삼성전자", "author": "w",
        "created_at": "2026-08-17 00:00", "views": 1, "recommendations": 1, "reply_count_reported": 1,
        "content_text": "본문", "content_html": "<p>본문</p>", "raw_html": "<html>raw</html>",
        "raw_html_storage_path": "mlbpark/samsung/posts/123.html.gz", "matched_keywords": '["삼성전자"]',
    }
    comments = [{
        "post_id": "123", "comment_id": "1", "author": "r", "created_at": "now", "body_text": "댓글",
        "parent_author": None, "raw_html": "<div>raw</div>",
        "raw_html_storage_path": "mlbpark/samsung/posts/123.html.gz",
    }]
    crawler.local_commit(clean_db, post, comments)
    assert clean_db.execute("SELECT raw_html_storage_path FROM posts WHERE post_id='123'").fetchone()[0] == post["raw_html_storage_path"]
    assert clean_db.execute("SELECT raw_html_storage_path FROM comments WHERE post_id='123' AND comment_id='1'").fetchone()[0] == post["raw_html_storage_path"]


def test_supabase_upsert_excludes_raw_html():
    sink = MagicMock()
    post = {
        "post_id": "123", "matched_keywords": "[]", "raw_html": "raw", "content_html": "html",
    }
    crawler.upload_post_to_external(post, [], sink)
    sent_post = sink.upsert.call_args.args[1][0]
    assert "raw_html" not in sent_post
    assert "content_html" not in sent_post


# ==========================================
# Rev8: InvalidPostIdentity — None actual_post_id
# ==========================================
def test_T_PostId_None_mismatch():
    """① actual_post_id가 None이면 InvalidPostIdentity 발생해야 함.
    이전: `if actual_post_id and ...` 로 None을 통과시킴.
    수정 후: `if actual_post_id != requested_post_id` 로 None도 불일치 처리."""
    page = MagicMock()
    page.url = "https://mlbpark.donga.com/mp/b.php?p=1&b=bullpen"  # id 없음
    page.css = MagicMock(return_value=MagicMock(
        get=MagicMock(return_value=""),
        get_all=MagicMock(return_value=[]),
    ))
    page.body = "<html></html>"

    with pytest.raises(crawler.InvalidPostIdentity, match="Identity mismatch"):
        crawler.extract_post(
            page,
            "https://mlbpark.donga.com/mp/b.php?m=view&b=bullpen&id=12345678",
            ["test"]
        )


def test_category_comes_from_page_metadata():
    assert crawler.extract_category_from_page_html("window.x = { 'head': '주식' };") == "주식"
    assert crawler.extract_category_from_page_html('window.x = { "head": "야구" };') == "야구"
    assert crawler.extract_category_from_page_html("<html></html>") is None


class SearchRow:
    def __init__(self, href, date_text):
        self.href = href
        self.date_text = date_text

    def css(self, selector):
        value = self.href if "href" in selector else self.date_text
        return MagicMock(get=MagicMock(return_value=value))


class SearchPage:
    def __init__(self, rows):
        self.rows = rows

    def css(self, selector):
        if selector == "tbody tr":
            return self.rows
        raise AssertionError(f"Unexpected page-level selector: {selector}")


def test_search_rows_are_scoped_and_ignore_sidebar_dates():
    page = SearchPage([
        SearchRow("/mp/b.php?m=view&b=bullpen&id=202608170118028469", "07:59:06"),
        SearchRow("/mp/b.php?m=view&b=bullpen&id=202608160118008042", "2026-08-16"),
    ])
    rows = crawler.extract_search_result_rows(page, today="2026-08-17")
    assert [(row["post_id"], row["date"]) for row in rows] == [
        ("202608170118028469", "2026-08-17"),
        ("202608160118008042", "2026-08-16"),
    ]
    assert crawler.parse_search_result_date("2시간전", today="2026-08-17") is None
    assert crawler.parse_search_result_date("2026.07월", today="2026-08-17") is None


def test_discovery_cursor_advances_beyond_old_page_cap(clean_db):
    clean_db.execute(
        "INSERT INTO discovery_progress (keyword, last_page_scanned, completed_at, updated_at) VALUES (?, ?, ?, ?)",
        ("삼성전자", 899, None, crawler.now_iso()),
    )
    clean_db.commit()
    urls = []

    def fake_fetch(url, *args, **kwargs):
        urls.append(url)
        return SearchPage([])

    with patch("mlbpark_scrapling_collect.fetch", side_effect=fake_fetch), patch("mlbpark_scrapling_collect.polite_sleep"):
        crawler.discover_phase(clean_db, "삼성전자", max_pages=2, timeout=10, delay=0)

    assert "p=27001" in urls[0]
    assert "p=27031" in urls[1]
    assert clean_db.execute("SELECT last_page_scanned FROM discovery_progress WHERE keyword='삼성전자'").fetchone()[0] == 901


# ==========================================
# Rev8: end_date skip → skipped 상태 (not done)
# ==========================================
def test_T_EndDate_Skip(clean_db):
    """② end_date 범위 밖 게시글 → skipped 상태.
    done이 아닌 skipped로 전환되어 done_without_post=0 불변식 보존."""
    insert_post_queue(clean_db, "12345678")

    with patch('mlbpark_scrapling_collect.fetch') as mock_fetch:
        mock_page = DummyPage("<html><body>test</body></html>")
        # extract_post가 범위 밖 날짜를 반환하도록 mock
        mock_fetch.return_value = mock_page

        with patch('mlbpark_scrapling_collect.extract_post') as mock_extract:
            mock_extract.return_value = {
                "post_id": "12345678", "url": "a", "category": "b", "title": "c",
                "author": "d", "created_at": "2025-06-01 14:00", "views": 1,
                "recommendations": 0, "reply_count_reported": 0, "content_text": "f",
                "content_html": "g", "raw_html": "h", "matched_keywords": "[]"
            }

            with patch('sys.argv', ['test', '--db', DB_PATH, '--keywords', 'test', '--crawl-scope', 'test',
                        '--limit-posts', '1', '--end-date', '2025-01-01',
                        ]):
                crawler.main()

    # skipped 상태 확인
    cur = clean_db.execute("SELECT status, last_error FROM post_queue WHERE post_id='12345678'")
    row = cur.fetchone()
    assert row[0] == 'skipped', f"Expected 'skipped' but got '{row[0]}'"
    assert 'after end_date' in (row[1] or ''), f"Error msg should mention end_date, got: {row[1]}"

    # posts 테이블에 행 없음 → done_without_post 불변식 보존
    cur = clean_db.execute("SELECT COUNT(*) FROM posts WHERE post_id='12345678'")
    assert cur.fetchone()[0] == 0, "Skipped post should NOT have a posts row"

    # done_without_post=0 불변식 검증
    cur = clean_db.execute("""
        SELECT COUNT(*) FROM post_queue q
        LEFT JOIN posts p ON q.post_id = p.post_id
        WHERE q.status = 'done' AND p.post_id IS NULL
    """)
    assert cur.fetchone()[0] == 0, "done_without_post invariant violated"


# ==========================================
# Rev8: Supabase 크레덴셜 부분 누락 → SystemExit
# ==========================================
def test_T_Supa_PartialCred():
    """③ Supabase url만 지정 + key 누락 → SystemExit (fail-loud)."""
    args = MagicMock()
    args.supabase_url = "https://supabase.example.com"
    args.supabase_key = None

    with patch.dict('os.environ', {}, clear=True):
        with pytest.raises(SystemExit, match="missing"):
            crawler.make_supabase_sink(args)


def test_T_Supa_NeitherSet():
    """③ Supabase 둘 다 미지정 → None (의도적 미사용, 에러 아님)."""
    args = MagicMock()
    args.supabase_url = None
    args.supabase_key = None

    with patch.dict('os.environ', {}, clear=True):
        assert crawler.make_supabase_sink(args) is None


def test_queue_transition_is_persisted_immediately_with_scope(clean_db):
    insert_post_queue(clean_db, "12345678")
    sink = MagicMock()

    crawler.transition_queue(
        clean_db, "12345678", "processing", supabase=sink, crawl_scope="pilot-a"
    )

    table, rows, conflict = sink.upsert.call_args.args
    assert table == "mlbpark_python_post_queue"
    assert conflict == "crawl_scope,post_id"
    assert rows[0]["crawl_scope"] == "pilot-a"
    assert rows[0]["status"] == "processing"


def test_hydrate_restores_only_requested_scope_and_page_zero(clean_db):
    sink = MagicMock()
    sink.select_all.side_effect = [
        [{"keyword": "삼성전자", "next_page": 0, "completed_at": None, "updated_at": "now"}],
        [{"post_id": "42", "canonical_url": crawler.build_canonical_url("42"), "status": "retry", "attempt_count": 2, "claimed_at": None, "last_error": "x"}],
    ]

    crawler.hydrate_durable_state(clean_db, sink, ["삼성전자"], "pilot-a")

    assert clean_db.execute("SELECT last_page_scanned FROM discovery_progress WHERE keyword='삼성전자'").fetchone()[0] == -1
    assert clean_db.execute("SELECT status,restored_at_start FROM post_queue WHERE post_id='42'").fetchone() == ("retry", 1)
    assert all("crawl_scope=eq.pilot-a" in call.args[1] for call in sink.select_all.call_args_list)


def test_supabase_select_all_paginates_past_postgrest_default():
    sink = crawler.SupabaseSink("https://example.supabase.co", "key")
    first = MagicMock(status_code=200)
    first.json.return_value = [{"n": i} for i in range(2)]
    second = MagicMock(status_code=200)
    second.json.return_value = [{"n": 2}]
    sink.session.get = MagicMock(side_effect=[first, second])

    assert len(sink.select_all("queue", page_size=2)) == 3
    assert sink.session.get.call_args_list[1].kwargs["headers"]["Range"] == "2-3"


def test_final_checkpoint_does_not_reupload_whole_queue(clean_db):
    insert_post_queue(clean_db, "123")
    clean_db.execute(
        "insert into discovery_progress(keyword,last_page_scanned,updated_at) values('삼성전자',3,?)",
        (crawler.now_iso(),),
    )
    clean_db.commit()
    sink = MagicMock()

    crawler.persist_durable_state(clean_db, sink, "scope-a")

    assert sink.upsert.call_count == 1
    assert sink.upsert.call_args.args[0] == "mlbpark_python_crawl_state"


def test_429_stops_after_first_request_and_preserves_retry_after():
    page = MagicMock(status=429, headers={"Retry-After": "120"})
    with patch("mlbpark_scrapling_collect.Fetcher.get", return_value=page) as getter:
        with pytest.raises(crawler.CrawlBlockedError) as raised:
            crawler.fetch("https://example.test", timeout=5, retries=5, backoff=0)
    assert getter.call_count == 1
    assert raised.value.retry_after == "120"


def test_collected_comment_count_is_stored_separately(clean_db):
    post = {
        "post_id": "123", "url": "a", "category": "주식", "title": "삼성전자", "author": "w",
        "created_at": "2026-08-17 00:00", "views": 1, "recommendations": 1,
        "reply_count_reported": 9, "collected_comment_count": 1,
        "content_text": "본문", "content_html": "<p>본문</p>", "raw_html": "<html>raw</html>",
        "matched_keywords": '["삼성전자"]',
    }
    comments = [{"post_id": "123", "comment_id": "1", "author": "r", "created_at": "now", "body_text": "댓글", "parent_author": None, "raw_html": "<div>raw</div>"}]
    crawler.local_commit(clean_db, post, comments)
    assert clean_db.execute("SELECT reply_count_reported,collected_comment_count FROM posts WHERE post_id='123'").fetchone() == (9, 1)


def test_fresh_schema_has_no_sync_status_columns(clean_db):
    columns = {row[1] for row in clean_db.execute("pragma table_info(posts)")}
    assert "r2_sync_status" not in columns
    assert "supabase_sync_status" not in columns


def test_real_html_fixture_locks_post_comment_and_blank_reply_count_parsing():
    fixture = Path(__file__).parent / "fixtures" / "mlbpark_post_stock_excerpt.html"
    html = fixture.read_text(encoding="utf-8")
    url = crawler.build_canonical_url("202608170118028469")
    page = Adaptor(text=html, body=html.encode("utf-8"), url=url)

    post = crawler.extract_post(page, url, ["삼성전자", "반도체"])
    comments = crawler.extract_comments(page, post["post_id"])

    assert post["category"] == "주식"
    assert post["created_at"] == "2026-08-17 07:59"
    assert post["views"] == 4483
    assert post["recommendations"] == 0
    assert post["reply_count_reported"] is None
    assert "삼성전자 2.8조" in post["content_text"]
    assert json.loads(post["matched_keywords"]) == ["삼성전자"]
    assert [row["comment_id"] for row in comments] == ["32214633", "32214634"]
    assert comments[0]["body_text"] == "갑자기 왜케 폭풍매수 하는건지?"
