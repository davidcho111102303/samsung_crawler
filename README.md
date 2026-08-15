# MLBPARK Samsung Crawler for GitHub Actions

This repository runs the MLBPARK Samsung-related backfill crawler on GitHub Actions and writes results to Supabase.

Required repository secrets:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Manual run:

1. Open GitHub Actions.
2. Select `MLBPARK Samsung Backfill`.
3. Run workflow with:
   - `start_date`: `2024-01-01`
   - `end_date`: `2026-08-12`
   - `keywords`: `삼전 삼성전자 반도체 삼성`
   - `max_search_pages`: `900`

The crawler stores structured rows in Supabase and compressed raw HTML in Supabase Storage. Local raw HTML is not retained.
