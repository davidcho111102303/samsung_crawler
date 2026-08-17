#!/usr/bin/env python3
import json
import sqlite3
import sys


def scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


def main(path):
    conn = sqlite3.connect(path)
    checks = {
        "posts": scalar(conn, "select count(*) from posts"),
        "comments": scalar(conn, "select count(*) from comments"),
        "duplicate_posts": scalar(conn, "select count(*) from (select post_id from posts group by post_id having count(*) > 1)"),
        "duplicate_comments": scalar(conn, "select count(*) from (select post_id,comment_id from comments group by post_id,comment_id having count(*) > 1)"),
        "done_without_post": scalar(conn, "select count(*) from post_queue q left join posts p using(post_id) where q.status='done' and q.restored_at_start=0 and p.post_id is null"),
        "non_stock_posts": scalar(conn, "select count(*) from posts where category is distinct from '주식'"),
        "missing_storage_path": scalar(conn, "select count(*) from posts where raw_html_storage_path is null or raw_html_storage_path=''"),
        "comment_count_mismatch": scalar(conn, "select count(*) from posts p where collected_comment_count != (select count(*) from comments c where c.post_id=p.post_id)"),
    }
    print(json.dumps(checks, ensure_ascii=False, sort_keys=True))
    failures = {key: value for key, value in checks.items() if key not in {"posts", "comments"} and value}
    if failures:
        raise SystemExit(f"dataset invariant failure: {failures}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: validate_dataset.py PATH.sqlite")
    main(sys.argv[1])
