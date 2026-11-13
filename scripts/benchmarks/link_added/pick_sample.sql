-- The 50-page sample of the link-addition benchmark: pages with at least one reference, ten per
-- band of revision count (1-9, 10-99, 100-999, 1,000-9,999, 10,000+), one drawn at random from each
-- tenth of the band by reference count, with a fixed seed. Run with psql from the folder to write
-- sample.csv there:  psql "$DATABASE_URL" -f pick_sample.sql
SELECT setseed(0.42);
\copy (WITH s AS (SELECT page_id, revision_count, reference_count, latest_rev_id, width_bucket(revision_count, array[1,10,100,1000,10000]) AS stratum FROM page_summary_cache WHERE domain_id = 1 AND reference_count > 0), t AS (SELECT *, ntile(10) OVER (PARTITION BY stratum ORDER BY reference_count) AS tile FROM s), r AS (SELECT *, random() AS rnd FROM t) SELECT DISTINCT ON (stratum, tile) stratum, tile, page_id, revision_count, reference_count, latest_rev_id FROM r ORDER BY stratum, tile, rnd) TO 'sample.csv' WITH CSV HEADER
