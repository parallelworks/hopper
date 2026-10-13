-- hopper schema v11: batch progress without a shared counter.
--
-- Through v10 every statement that finalized a batch's jobs counted the
-- batch down on its hopper_batches row, so the finalizes of a batch's jobs
-- queued on that row, and a transaction that finalized a job and stayed open
-- (CompleteTx) held up all of its siblings. From v11 a finalizing statement
-- appends a row here instead, which takes no lock anyone else waits for. A
-- batch's remaining count is its pending column less the rows' done; the
-- statement that finds nothing remaining completes the batch. The leader
-- folds the rows into their batch rows, which keeps this table to the last
-- few seconds of progress, and completes a batch whose last jobs were
-- finalized by concurrent statements, each of which saw the other's row
-- uncommitted.
--
-- A new table takes no lock on an existing one, and a client of the
-- previous release keeps counting down pending, which the remaining count
-- includes.

CREATE TABLE hopper_batch_progress (
  id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  batch_id uuid NOT NULL,
  done     int  NOT NULL,  -- jobs finalized
  failed   int  NOT NULL   -- of those, cancelled or discarded
);

CREATE INDEX hopper_batch_progress_batch ON hopper_batch_progress (batch_id);
