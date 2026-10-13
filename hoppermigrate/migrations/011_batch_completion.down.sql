-- The previous release counts batches down on pending alone: fold the
-- progress rows into it. A batch whose count reaches zero here completes,
-- with its callbacks, as the previous release's finalize would have done.

WITH sums AS (
  SELECT batch_id, sum(done) AS done, sum(failed) AS failed FROM hopper_batch_progress GROUP BY batch_id
),
folded AS (
  UPDATE hopper_batches b
  SET pending = b.pending - s.done, failed = b.failed + s.failed,
      completed_at = CASE WHEN b.completed_at IS NULL AND b.pending - s.done <= 0 THEN now() ELSE b.completed_at END
  FROM sums s WHERE b.id = s.batch_id
  RETURNING b.id, b.failed, b.on_success, b.on_failure, b.on_complete, b.completed_at = now() AS completed_now
)
INSERT INTO hopper_jobs (kind, queue, priority, max_attempts, args, metadata)
SELECT cb->>'kind', coalesce(cb->>'queue', 'default'), coalesce((cb->>'priority')::smallint, 2),
  coalesce((cb->>'max_attempts')::smallint, 25), coalesce(cb->'args', '{}'),
  coalesce(cb->'metadata', '{}') || jsonb_build_object('batch_id', b.id::text, 'batch_failed', b.failed)
FROM folded b, LATERAL (VALUES (CASE WHEN b.failed = 0 THEN b.on_success ELSE b.on_failure END), (b.on_complete)) AS v(cb)
WHERE b.completed_now AND cb IS NOT NULL;

DROP TABLE hopper_batch_progress;
