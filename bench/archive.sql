-- Parquet archive of raw BGL (4,747,963 lines): raw lines vs cluster_id + params + a template table.
-- duckdb -unsigned -c ".read bench/archive.sql" (from the repo root, with DRAIN3_BENCH data at the path below).
-- All files use Parquet with ZSTD compression.
LOAD 'build/release/extension/drain3/drain3.duckdb_extension';
SET VARIABLE bgl = getenv('DRAIN3_BENCH') || '/data/BGL.log';
CREATE TABLE logs AS
SELECT row_number() OVER () AS ln, message
FROM read_csv(getvariable('bgl'), columns = {'message': 'VARCHAR'}, delim = '\t', quote = '', escape = '',
              header = false, auto_detect = false);
COPY (SELECT message FROM logs ORDER BY ln) TO '/tmp/drain3-archive/raw.parquet' (COMPRESSION zstd);
COPY (SELECT cluster_id, params FROM drain_parse('logs', 'message', order_by := 'ln')) TO '/tmp/drain3-archive/lines.parquet' (COMPRESSION zstd);
COPY (SELECT unnest(drain_clusters(drain_train(message ORDER BY ln)), recursive := true) FROM logs) TO '/tmp/drain3-archive/templates.parquet' (COMPRESSION zstd);
SELECT
  (SELECT count(*) FROM logs) AS lines,
  (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/drain3-archive/raw.parquet')) AS raw_parquet_bytes,
  (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/drain3-archive/lines.parquet'))
  + (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/drain3-archive/templates.parquet')) AS archive_bytes;
-- Lines rebuilt from the archive equal the originals up to whitespace (templates use single spaces).
SELECT count(*) FILTER (WHERE drain_render(t.template, l.params) = regexp_replace(trim(o.message), '\s+', ' ', 'g')) AS rebuilt_equal,
       count(*) AS total
FROM (SELECT row_number() OVER () AS ln, * FROM '/tmp/drain3-archive/lines.parquet') l
JOIN '/tmp/drain3-archive/templates.parquet' t USING (cluster_id)
JOIN logs o USING (ln);
