-- MySQL 5.7: run on a read-only replica with the application database selected.
-- This SELECT generates one read-only query per monthly archive. Execute each
-- returned audit_sql separately. No parent join filter: orphan hot buckets must
-- also be reported. A zero amount does not identify a partial bucket.
SELECT CONCAT(
    'SELECT ''', tables.TABLE_NAME, ''' AS archive_table, ',
    'stats.session_id, stats.bucket_index, ',
    'stats.month AS hot_month, archived.month AS archived_month, ',
    'parent.end_time AS hot_parent_end_time, ',
    'stats.gift AS hot_gift, archived.gift AS archived_gift, ',
    'stats.guard AS hot_guard, archived.guard AS archived_guard, ',
    'stats.super_chat AS hot_super_chat, archived.super_chat AS archived_super_chat, ',
    'stats.payer_count AS hot_payer_count, ',
    IF(MAX(columns.COLUMN_NAME = 'payer_count'), 'archived.payer_count', 'NULL'),
    ' AS archived_payer_count, stats.danmaku_count AS hot_danmaku_count, ',
    IF(MAX(columns.COLUMN_NAME = 'danmaku_count'), 'archived.danmaku_count', 'NULL'),
    ' AS archived_danmaku_count ',
    'FROM live_session_15m_stats AS stats JOIN `', tables.TABLE_NAME, '` AS archived ',
    'ON archived.session_id = stats.session_id AND archived.bucket_index = stats.bucket_index ',
    'LEFT JOIN live_session AS parent ON parent.id = stats.session_id ',
    'ORDER BY stats.session_id, stats.bucket_index;'
) AS audit_sql
FROM information_schema.TABLES AS tables
JOIN information_schema.COLUMNS AS columns
  ON columns.TABLE_SCHEMA = tables.TABLE_SCHEMA
  AND columns.TABLE_NAME = tables.TABLE_NAME
WHERE tables.TABLE_SCHEMA = DATABASE()
  AND tables.TABLE_NAME REGEXP '^live_session_15m_stats_[0-9]{6}$'
GROUP BY tables.TABLE_NAME
ORDER BY tables.TABLE_NAME;
