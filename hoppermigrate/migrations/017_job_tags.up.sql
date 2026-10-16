-- hopper schema v17: tags, for finding jobs by label.
--
-- A job's tags are a JSON array of strings on its row, NULL when it has
-- none, so that untagged jobs store nothing for them, and they are kept in
-- history like the rest of the job. A CHECK on both tables keeps a direct
-- SQL write from storing anything else; it is NOT VALID, so adding it reads
-- no rows (none has tags yet).
--
-- JobList finds a tag's jobs through tables of (tag, id) rows, one for live
-- jobs and one for history, whose btree walks a tag's jobs in ID order from
-- the listing's cursor: a page costs about its size, for a rare tag or a
-- common one. Triggers keep them in step with the tags column on every
-- write, whoever makes it. The history table is
-- partitioned as hopper_job_history is, and the leader creates and drops
-- its time partitions with history's, so retention stays a partition drop.
--
-- The column and the triggers are added like v15's progress: with a short
-- lock_timeout per try, in a loop. The timeout is the loop's alone: the
-- statements before and after it wait for a lock as any statement does,
-- rather than fail the migration on a brief conflict.

-- A tag is a label matched by equality alone, so its column compares bytes
-- (COLLATE "C"): the tables' btrees then use no locale's comparison, which
-- costs more and whose order an upgrade of the collation library can change
-- under an index, leaving it to be rebuilt.
CREATE TABLE hopper_job_tags (
  tag text COLLATE "C" NOT NULL,
  id  uuid NOT NULL,
  PRIMARY KEY (tag, id)
) WITH (
  -- Rows come and go with tagged jobs, as hopper_jobs' do (v6, v7).
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 1000,
  autovacuum_vacuum_cost_delay = 0,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_insert_scale_factor = 0.05,
  vacuum_truncate = false
);

CREATE TABLE hopper_job_history_tags (
  state        hopper_job_state NOT NULL,
  finalized_at timestamptz      NOT NULL,
  tag          text COLLATE "C" NOT NULL,
  id           uuid             NOT NULL
) PARTITION BY LIST (state);
CREATE TABLE hopper_job_history_tags_completed PARTITION OF hopper_job_history_tags
  FOR VALUES IN ('completed') PARTITION BY RANGE (finalized_at);
CREATE TABLE hopper_job_history_tags_failed PARTITION OF hopper_job_history_tags
  FOR VALUES IN ('canceled', 'discarded') PARTITION BY RANGE (finalized_at);
CREATE TABLE hopper_job_history_tags_completed_default PARTITION OF hopper_job_history_tags_completed DEFAULT;
CREATE TABLE hopper_job_history_tags_failed_default PARTITION OF hopper_job_history_tags_failed DEFAULT;
-- A job's tags rows are unique, as hopper_job_tags' key keeps live ones: a
-- second copy, which a TRUNCATE of a history partition (which fires none of
-- history's triggers) followed by a reinsert of its rows would otherwise
-- add, would list the job twice. The key includes the partition keys, as
-- Postgres requires, and still walks a tag's jobs in ID order.
CREATE UNIQUE INDEX hopper_job_history_tags_tag ON hopper_job_history_tags (tag, id, state, finalized_at);

-- The time partitions history already has for the current period and those
-- ahead get their tags partitions here, so that a job's tags rows sit in the
-- partition its job does from the start; the leader creates later ones
-- beside history's. A past period's partition gets none: a job is
-- finalized into history at now(), so only a direct SQL write could give it
-- tags rows, which the tags' DEFAULT partition keeps instead, and a
-- retention of months would otherwise create a table, with its index, for
-- each hour of it in this one transaction. The bound is read from the
-- catalog alone (pg_get_expr without the relation, which would open it): a
-- partition the leader holds while it creates or drops one is listed
-- without waiting for it. Its period's end is checked in the loop, not in the
-- query, whose planner could otherwise read the bound of every relation in
-- the catalog, another schema's being dropped included. The bound goes
-- through text, so it is written in ISO's form, with a numeric offset:
-- another DateStyle writes the time zone's abbreviation, which Postgres
-- before 18 reads by timezone_abbreviations alone (Asia/Shanghai's CST as US
-- Central's), and the copy would cover a period hours off its name's.
DO $$
DECLARE
  p record;
  prior_datestyle constant text := current_setting('datestyle');
BEGIN
  PERFORM set_config('datestyle', 'ISO', true);
  FOR p IN
    SELECT c.relname AS name, pc.relname AS parent, pg_get_expr(c.relpartbound, 0) AS bound
    FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class pc ON pc.oid = i.inhparent
    JOIN pg_partitioned_table pt ON pt.partrelid = i.inhparent
    WHERE i.inhparent IN ('hopper_job_history_completed'::regclass, 'hopper_job_history_failed'::regclass)
      AND c.oid <> pt.partdefid
  LOOP
    IF (regexp_match(p.bound, 'TO \(''([^'']+)''\)'))[1]::timestamptz > now() THEN
      EXECUTE format('CREATE TABLE %I PARTITION OF %I %s',
        'hopper_job_history_tags_' || substr(p.name, length('hopper_job_history_') + 1),
        'hopper_job_history_tags_' || substr(p.parent, length('hopper_job_history_') + 1), p.bound);
    END IF;
  END LOOP;
  PERFORM set_config('datestyle', prior_datestyle, true);
END
$$;

-- hopper_tags_of reads the non-empty strings of a tags value, once each. Both
-- tables' CHECK keeps tags an array of strings, but not the empty string or
-- repeats from a direct SQL write. The type tests are redundant under the
-- CHECK, but the shorter form over jsonb_array_elements_text made tagged
-- throughput a quarter of this one's in hopperbench.
CREATE FUNCTION hopper_tags_of(tags jsonb) RETURNS SETOF text
LANGUAGE sql IMMUTABLE
AS $$
  SELECT DISTINCT e #>> '{}' FROM jsonb_array_elements(CASE WHEN jsonb_typeof(tags) = 'array' THEN tags END) e
  WHERE jsonb_typeof(e) = 'string' AND e <> '""'
$$;

-- An insert adds a job's tags rows from a row trigger that fires only for
-- a tagged job: an insert's transition table would copy every new row,
-- payload and all, untagged or not. New jobs have no tags rows yet, so the
-- insert needs no conflict check, which cost three times the insert itself.
-- A statement that deletes a tagged job and inserts it again under the same
-- ID fails with a unique violation on hopper_job_tags, the delete's trigger
-- firing only at the statement's end, and changes nothing; the same job
-- is written back by an UPDATE instead. A data-only restore runs with
-- triggers disabled, or without the tags tables.
--
-- A delete from hopper_jobs removes them from a statement trigger, which
-- reads the statement's rows from a transition table that keeps large values
-- where they are stored, so a batch of finalizes costs one statement, not
-- one per job. That delete is planned on
-- each call when it has tagged rows, since a plan cached for one batch's size
-- is wrong for another's. An update of a live job's tags, which only a
-- replacing insert makes, is kept by a row trigger, as is one of its ID,
-- which keys its tags rows too.
--
-- History's triggers are row triggers, all of them: Postgres gives a
-- partitioned table's row triggers to each of its partitions, present and
-- future, so they fire for a write aimed at an outcome table or a partition
-- as well as at the root, where a statement trigger fires only for its own
-- table. History is deleted from and updated only by retries, retention's
-- prune of its DEFAULT partitions, and direct SQL, so a row each costs
-- little, and an untagged one nothing. Its tags rows are keyed by the job's
-- state and finalized_at as well, and an update that changes either can move
-- the job to another partition, which Postgres runs as a delete from the old
-- one and an insert into the new, firing those triggers; an update that
-- stays in its partition fires the update trigger. A TRUNCATE of a single
-- partition fires no trigger of the root's: the tags rows it leaves are
-- dropped with their partition, or pruned from the DEFAULT one, by
-- retention, and a listing checks each job it finds for the tag.
CREATE FUNCTION hopper_job_tags_inserted() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO hopper_job_tags (tag, id) SELECT tag, NEW.id FROM hopper_tags_of(NEW.tags) tag;
  RETURN NULL;
END
$$;

CREATE FUNCTION hopper_job_tags_deleted() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (SELECT FROM hopper_jobs_deleted WHERE tags IS NOT NULL) THEN
    EXECUTE 'DELETE FROM hopper_job_tags t USING hopper_jobs_deleted j, hopper_tags_of(j.tags) o(tag)
             WHERE j.tags IS NOT NULL AND t.tag = o.tag AND t.id = j.id';
  END IF;
  RETURN NULL;
END
$$;

CREATE FUNCTION hopper_job_tags_updated() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.id = NEW.id THEN
    DELETE FROM hopper_job_tags
    WHERE id = OLD.id AND tag = ANY (ARRAY(SELECT hopper_tags_of(OLD.tags) EXCEPT SELECT hopper_tags_of(NEW.tags)));
    INSERT INTO hopper_job_tags (tag, id)
    SELECT tag, NEW.id FROM (SELECT hopper_tags_of(NEW.tags) EXCEPT SELECT hopper_tags_of(OLD.tags)) added(tag);
  ELSE
    DELETE FROM hopper_job_tags WHERE id = OLD.id AND tag = ANY (ARRAY(SELECT hopper_tags_of(OLD.tags)));
    INSERT INTO hopper_job_tags (tag, id) SELECT tag, NEW.id FROM hopper_tags_of(NEW.tags) tag;
  END IF;
  RETURN NULL;
END
$$;

CREATE FUNCTION hopper_job_history_tags_inserted() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO hopper_job_history_tags (state, finalized_at, tag, id)
  SELECT NEW.state, NEW.finalized_at, tag, NEW.id FROM hopper_tags_of(NEW.tags) tag;
  RETURN NULL;
END
$$;

CREATE FUNCTION hopper_job_history_tags_deleted() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM hopper_job_history_tags
  WHERE state = OLD.state AND finalized_at = OLD.finalized_at AND id = OLD.id
    AND tag = ANY (ARRAY(SELECT hopper_tags_of(OLD.tags)));
  RETURN NULL;
END
$$;

CREATE FUNCTION hopper_job_history_tags_updated() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM hopper_job_history_tags
  WHERE state = OLD.state AND finalized_at = OLD.finalized_at AND id = OLD.id
    AND tag = ANY (ARRAY(SELECT hopper_tags_of(OLD.tags)));
  INSERT INTO hopper_job_history_tags (state, finalized_at, tag, id)
  SELECT NEW.state, NEW.finalized_at, tag, NEW.id FROM hopper_tags_of(NEW.tags) tag;
  RETURN NULL;
END
$$;
-- The functions resolve this schema's tables whatever the writer's
-- search_path, as v8's do. Saving and restoring the setting on each call
-- measured no slower than leaving it out (20,000 tagged inserts and
-- deletes, best of 8 runs each: 249.0 against 249.1 ms).
ALTER FUNCTION hopper_job_tags_inserted() SET search_path FROM CURRENT;
ALTER FUNCTION hopper_job_tags_deleted() SET search_path FROM CURRENT;
ALTER FUNCTION hopper_job_tags_updated() SET search_path FROM CURRENT;
ALTER FUNCTION hopper_job_history_tags_inserted() SET search_path FROM CURRENT;
ALTER FUNCTION hopper_job_history_tags_deleted() SET search_path FROM CURRENT;
ALTER FUNCTION hopper_job_history_tags_updated() SET search_path FROM CURRENT;

-- TRUNCATE fires no delete trigger, so a truncate of hopper_jobs or of
-- history truncates its tags table too. A statement that truncates both, as
-- one that clears every table does, has emptied the tags table by the time
-- the trigger runs, and truncating it again in that statement would fail
-- (the statement holds it open), so an empty one is left alone.
CREATE FUNCTION hopper_job_tags_truncated() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (SELECT FROM hopper_job_tags) THEN
    TRUNCATE hopper_job_tags;
  END IF;
  RETURN NULL;
END
$$;

CREATE FUNCTION hopper_job_history_tags_truncated() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (SELECT FROM hopper_job_history_tags) THEN
    TRUNCATE hopper_job_history_tags;
  END IF;
  RETURN NULL;
END
$$;
ALTER FUNCTION hopper_job_tags_truncated() SET search_path FROM CURRENT;
ALTER FUNCTION hopper_job_history_tags_truncated() SET search_path FROM CURRENT;

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
  prior_lock_timeout constant text := current_setting('lock_timeout');
BEGIN
  PERFORM set_config('lock_timeout', '50ms', true);
  LOOP
    BEGIN
      -- Both tables are locked before the first ALTER, hopper_jobs first, the
      -- order a finalize takes them in. Locking history first left a finalize
      -- that had locked hopper_jobs waiting for history meanwhile, and the
      -- try waiting for that finalize until its lock_timeout: under a steady
      -- stream of finalizes, nearly every try.
      LOCK TABLE hopper_jobs, hopper_job_history IN ACCESS EXCLUSIVE MODE;
      -- strict: in lax mode the filter unwraps a nested array, so that
      -- [["a"]] and [[]] would pass as arrays of strings.
      ALTER TABLE hopper_job_history ADD COLUMN tags jsonb,
        ADD CONSTRAINT hopper_job_history_tags_strings
          CHECK (jsonb_typeof(tags) = 'array' AND NOT jsonb_path_exists(tags, 'strict $[*] ? (@.type() != "string")'))
          NOT VALID;
      CREATE TRIGGER hopper_job_history_tags_insert AFTER INSERT ON hopper_job_history
        FOR EACH ROW WHEN (NEW.tags IS NOT NULL) EXECUTE FUNCTION hopper_job_history_tags_inserted();
      CREATE TRIGGER hopper_job_history_tags_delete AFTER DELETE ON hopper_job_history
        FOR EACH ROW WHEN (OLD.tags IS NOT NULL) EXECUTE FUNCTION hopper_job_history_tags_deleted();
      CREATE TRIGGER hopper_job_history_tags_update AFTER UPDATE OF state, finalized_at, id, tags ON hopper_job_history
        FOR EACH ROW WHEN ((OLD.tags IS NOT NULL OR NEW.tags IS NOT NULL)
          AND (OLD.state, OLD.finalized_at, OLD.id, OLD.tags) IS DISTINCT FROM (NEW.state, NEW.finalized_at, NEW.id, NEW.tags))
        EXECUTE FUNCTION hopper_job_history_tags_updated();
      CREATE TRIGGER hopper_job_history_tags_truncate AFTER TRUNCATE ON hopper_job_history
        FOR EACH STATEMENT EXECUTE FUNCTION hopper_job_history_tags_truncated();
      ALTER TABLE hopper_jobs ADD COLUMN tags jsonb,
        ADD CONSTRAINT hopper_jobs_tags_strings
          CHECK (jsonb_typeof(tags) = 'array' AND NOT jsonb_path_exists(tags, 'strict $[*] ? (@.type() != "string")'))
          NOT VALID;
      CREATE TRIGGER hopper_jobs_tags_insert AFTER INSERT ON hopper_jobs
        FOR EACH ROW WHEN (NEW.tags IS NOT NULL) EXECUTE FUNCTION hopper_job_tags_inserted();
      CREATE TRIGGER hopper_jobs_tags_delete AFTER DELETE ON hopper_jobs
        REFERENCING OLD TABLE AS hopper_jobs_deleted
        FOR EACH STATEMENT EXECUTE FUNCTION hopper_job_tags_deleted();
      CREATE TRIGGER hopper_jobs_tags_update AFTER UPDATE OF tags, id ON hopper_jobs
        FOR EACH ROW WHEN (OLD.tags IS DISTINCT FROM NEW.tags OR OLD.id <> NEW.id) EXECUTE FUNCTION hopper_job_tags_updated();
      CREATE TRIGGER hopper_jobs_tags_truncate AFTER TRUNCATE ON hopper_jobs
        FOR EACH STATEMENT EXECUTE FUNCTION hopper_job_tags_truncated();
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_jobs and hopper_job_history to add tags in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_jobs or hopper_job_history; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_jobs and hopper_job_history (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
  PERFORM set_config('lock_timeout', prior_lock_timeout, true);
END
$$;

-- hopper_tags checks a SQL producer's tags as InsertOpts.Tags does: an
-- array of at most 64 distinct strings of 1 to 255 bytes without control
-- characters. Repeats are dropped, keeping the first, and none is stored as
-- NULL. Control characters are the code points Go's unicode.IsControl
-- names (U+0001 to U+001F and U+007F to U+009F), spelled out because what
-- [[:cntrl:]] matches depends on the database's collation: C1 controls in
-- C, and format characters such as an emoji's zero-width joiner in ICU.
-- A tag's bytes are counted in UTF-8, as Go counts them, whatever the
-- database's encoding: a tag of 255 LATIN1 bytes can be 510 in UTF-8, which
-- a Go client's filter would refuse. The control characters are looked for
-- in those UTF-8 bytes too, each pair of hex digits a byte: a regex sees a
-- single-byte encoding's bytes as code points, so that [\u0080-\u009f]
-- would match WIN1252's euro sign or WIN1251's Cyrillic letters, which are
-- those bytes there. In UTF-8 the C0 controls and DEL are bytes of their
-- own, and the C1 controls are C2 followed by 80 to 9F.
CREATE FUNCTION hopper_tags(tags jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
  kept jsonb := '[]';
  tag jsonb;
BEGIN
  IF tags IS NULL OR tags = 'null' THEN
    RETURN NULL;
  END IF;
  IF jsonb_typeof(tags) <> 'array' THEN
    RAISE EXCEPTION 'hopper: tags must be an array of strings' USING ERRCODE = 'invalid_parameter_value';
  END IF;
  FOR tag IN SELECT jsonb_array_elements(tags) LOOP
    IF jsonb_typeof(tag) <> 'string' OR tag #>> '{}' = '' OR octet_length(convert_to(tag #>> '{}', 'UTF8')) > 255
       OR encode(convert_to(tag #>> '{}', 'UTF8'), 'hex') ~ '^(..)*(0[1-9a-f]|1[0-9a-f]|7f|c2[89][0-9a-f])' THEN
      RAISE EXCEPTION 'hopper: tag % is not a string of 1 to 255 bytes without control characters', tag
        USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF NOT kept @> jsonb_build_array(tag) THEN
      IF jsonb_array_length(kept) = 64 THEN
        RAISE EXCEPTION 'hopper: a job has more than 64 tags' USING ERRCODE = 'invalid_parameter_value';
      END IF;
      kept := kept || jsonb_build_array(tag);
    END IF;
  END LOOP;
  RETURN nullif(kept, '[]');
END
$$;

-- hopper_insert takes tags too.
CREATE OR REPLACE FUNCTION hopper_insert(kind text, args jsonb, opts jsonb DEFAULT '{}') RETURNS uuid
LANGUAGE sql VOLATILE
SET search_path FROM CURRENT
AS $$
  INSERT INTO hopper_jobs (kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, unique_key,
                           ordering_key, expires_at, await, tags)
  VALUES (
    kind,
    coalesce(opts->>'queue', 'default'),
    (CASE WHEN (opts->>'scheduled_at')::timestamptz > now() THEN 'scheduled' ELSE 'available' END)::hopper_job_state,
    coalesce((opts->>'priority')::smallint, 2),
    coalesce((opts->>'max_attempts')::smallint, 25),
    coalesce((opts->>'scheduled_at')::timestamptz, now()),
    coalesce(args, '{}'),
    coalesce(opts->'metadata', '{}'),
    opts->>'unique_key',
    opts->>'ordering_key',
    CASE WHEN (opts->>'ttl_seconds')::float8 > 0 THEN now() + make_interval(secs => (opts->>'ttl_seconds')::float8) END,
    coalesce((opts->>'await')::boolean, false),
    hopper_tags(opts->'tags'))
  ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL DO UPDATE SET kind = EXCLUDED.kind
  RETURNING id
$$;
ALTER FUNCTION hopper_tags(jsonb) SET search_path FROM CURRENT;
