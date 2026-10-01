-- Run in the selected hareigift database after a full backup and stopping the collector.
-- MySQL 5.7/8.0. Default is a dry run; SET @vr_apply_changes=TRUE before SOURCE to commit.
-- DDL is deliberately outside the data transaction because MySQL implicitly commits DDL.
CREATE TABLE IF NOT EXISTS vr_repair_20261001_sessions LIKE live_session;
CREATE TABLE IF NOT EXISTS vr_repair_20261001_buckets LIKE live_session_15m_stats;
DROP PROCEDURE IF EXISTS vr_repair_live_sessions_202609;

DELIMITER $$
CREATE PROCEDURE vr_repair_live_sessions_202609(IN apply_changes BOOLEAN)
BEGIN
    DECLARE session_rows INT DEFAULT 0;
    DECLARE bucket_rows INT DEFAULT 0;
    DECLARE zombie_rows INT DEFAULT 0;
    DECLARE locked_end DATETIME DEFAULT '2026-09-30 18:00:20';
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;
    IF EXISTS (
        SELECT 1 FROM live_session WHERE month='202609' AND end_time IS NULL AND id<>17436
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Unexpected open September session; inspect before repair';
    END IF;
    IF EXISTS (
        SELECT 1 FROM live_session WHERE id=17436
        AND (room_id<>27628030 OR month<>'202609' OR start_time<>'2026-09-30 17:04:32')
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Session 17436 identity does not match the supplied dump';
    END IF;
    IF EXISTS (
        SELECT 1 FROM live_session p JOIN live_session_202609 a ON a.id=p.id WHERE p.month='202609'
    ) OR EXISTS (
        SELECT 1 FROM live_session_15m_stats s
        JOIN live_session_15m_stats_202609 a
          ON a.session_id=s.session_id AND a.bucket_index=s.bucket_index
        JOIN live_session p ON p.id=s.session_id WHERE p.month='202609'
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Hot/archive key collision; do not overwrite or ignore it';
    END IF;
    IF EXISTS (
        SELECT 1 FROM live_session_15m_stats s JOIN live_session p ON p.id=s.session_id
        WHERE p.month='202609' AND (s.month<>p.month OR s.room_id<>p.room_id)
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Unexpected child month/room mismatch';
    END IF;
    IF EXISTS (
        SELECT 1 FROM live_session_15m_stats
        WHERE session_id=17436 AND start_time>=locked_end
          AND (gift<>0 OR guard<>0 OR super_chat<>0 OR blind_box_count<>0
               OR blind_box_profit<>0 OR danmaku_count<>0 OR payer_count<>0
               OR COALESCE(captain_danmaku_count,0)<>0 OR COALESCE(admiral_danmaku_count,0)<>0
               OR COALESCE(governor_danmaku_count,0)<>0 OR COALESCE(normal_danmaku_count,0)<>0)
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Post-lock buckets contain activity; inspect instead of deleting';
    END IF;

    INSERT INTO vr_repair_20261001_sessions
    SELECT p.* FROM live_session p
    LEFT JOIN vr_repair_20261001_sessions b ON b.id=p.id
    WHERE p.month='202609' AND b.id IS NULL;
    INSERT INTO vr_repair_20261001_buckets
    SELECT s.* FROM live_session_15m_stats s JOIN live_session p ON p.id=s.session_id
    LEFT JOIN vr_repair_20261001_buckets b
      ON b.session_id=s.session_id AND b.bucket_index=s.bucket_index
    WHERE p.month='202609' AND b.session_id IS NULL;

    -- Post-lock concurrency-only samples are intentionally discarded, not treated as zero samples.
    -- The original 76 rows, including concurrency values, have been backed up above.
    DELETE FROM live_session_15m_stats
    WHERE session_id=17436 AND room_id=27628030 AND month='202609' AND start_time>=locked_end;
    SET zombie_rows=ROW_COUNT();
    -- The last bucket straddles the lock. Its exact pre-lock concurrency cannot be reconstructed.
    UPDATE live_session_15m_stats
    SET end_time=locked_end, avg_concurrency=NULL, max_concurrency=NULL, sample_count=0
    WHERE session_id=17436 AND room_id=27628030 AND month='202609'
      AND start_time<locked_end AND end_time>locked_end;
    UPDATE live_session SET end_time=locked_end, avg_concurrency=NULL, max_concurrency=NULL
    WHERE id=17436 AND room_id=27628030 AND month='202609' AND end_time IS NULL;

    INSERT INTO live_session_15m_stats_202609 (
        session_id,bucket_index,room_id,month,start_time,end_time,gift,guard,super_chat,
        blind_box_count,blind_box_profit,danmaku_count,avg_concurrency,max_concurrency,
        sample_count,payer_count,captain_danmaku_count,admiral_danmaku_count,
        governor_danmaku_count,normal_danmaku_count
    ) SELECT
        s.session_id,s.bucket_index,s.room_id,s.month,s.start_time,s.end_time,s.gift,s.guard,s.super_chat,
        s.blind_box_count,s.blind_box_profit,s.danmaku_count,s.avg_concurrency,s.max_concurrency,
        s.sample_count,s.payer_count,s.captain_danmaku_count,s.admiral_danmaku_count,
        s.governor_danmaku_count,s.normal_danmaku_count
    FROM live_session_15m_stats s JOIN live_session p ON p.id=s.session_id
    WHERE p.month='202609' AND p.end_time IS NOT NULL;
    SET bucket_rows=ROW_COUNT();
    DELETE s FROM live_session_15m_stats s JOIN live_session p ON p.id=s.session_id
    WHERE p.month='202609' AND p.end_time IS NOT NULL;

    INSERT INTO live_session_202609 (
        id,room_id,start_time,end_time,title,gift,guard,super_chat,danmaku_count,month,
        start_guard_1,start_guard_2,start_guard_3,start_fans_count,
        end_guard_1,end_guard_2,end_guard_3,end_fans_count,avg_concurrency,max_concurrency,
        blind_box_count,blind_box_profit,start_attention,end_attention,payer_count,
        captain_danmaku_count,admiral_danmaku_count,governor_danmaku_count,normal_danmaku_count
    ) SELECT
        id,room_id,start_time,end_time,title,gift,guard,super_chat,danmaku_count,month,
        start_guard_1,start_guard_2,start_guard_3,start_fans_count,
        end_guard_1,end_guard_2,end_guard_3,end_fans_count,avg_concurrency,max_concurrency,
        blind_box_count,blind_box_profit,start_attention,end_attention,payer_count,
        captain_danmaku_count,admiral_danmaku_count,governor_danmaku_count,normal_danmaku_count
    FROM live_session WHERE month='202609' AND end_time IS NOT NULL;
    SET session_rows=ROW_COUNT();
    DELETE FROM live_session WHERE month='202609' AND end_time IS NOT NULL;

    IF apply_changes THEN
        COMMIT;
    ELSE
        ROLLBACK;
    END IF;
    SELECT apply_changes AS committed, session_rows AS archived_sessions,
           bucket_rows AS archived_buckets, zombie_rows AS removed_zombie_buckets;
END$$
DELIMITER ;

CALL vr_repair_live_sessions_202609(COALESCE(@vr_apply_changes,FALSE));
DROP PROCEDURE vr_repair_live_sessions_202609;
SET @vr_apply_changes=NULL;

-- On the supplied dump the dry run reports 25 sessions, 400 buckets, 76 zombie buckets.
-- NULL end snapshots and concurrency stay unknown rather than using next-day observations.
-- October sessions, gift/guard/SC totals, room_live_stats and room_stats_monthly are untouched.
