-- hopper schema v17: tags, for finding jobs by label.
--
-- A job's tags are a JSON array of strings on its row, NULL when it has
-- none, and they are kept in history like the rest of the job. A CHECK keeps
-- a direct SQL write from storing anything but an array; a read skips what
-- in one is not a non-empty string, once each.
--
-- JobList finds a tag's jobs through tables of (tag, id) rows, one for live
-- jobs and one for history, whose primary keys walk a tag's jobs in ID
-- order. Triggers keep them in step with the tags column on every write,
-- whoever makes it.

ALTER TABLE hopper_job_history ADD COLUMN tags TEXT CHECK (tags IS NULL OR json_type(tags) = 'array');
ALTER TABLE hopper_jobs ADD COLUMN tags TEXT CHECK (tags IS NULL OR json_type(tags) = 'array');

CREATE TABLE hopper_job_tags (
  tag TEXT NOT NULL,
  id  BLOB NOT NULL,
  PRIMARY KEY (tag, id)
) WITHOUT ROWID;

CREATE TABLE hopper_job_history_tags (
  tag TEXT NOT NULL,
  id  BLOB NOT NULL,
  PRIMARY KEY (tag, id)
) WITHOUT ROWID;

CREATE TRIGGER hopper_jobs_tags_insert AFTER INSERT ON hopper_jobs
WHEN NEW.tags IS NOT NULL
BEGIN
  INSERT OR IGNORE INTO hopper_job_tags (tag, id)
  SELECT value, NEW.id FROM json_each(NEW.tags) WHERE type = 'text' AND value <> '';
END;

CREATE TRIGGER hopper_jobs_tags_delete AFTER DELETE ON hopper_jobs
WHEN OLD.tags IS NOT NULL
BEGIN
  DELETE FROM hopper_job_tags
  WHERE id = OLD.id AND tag IN (SELECT value FROM json_each(OLD.tags) WHERE type = 'text');
END;

CREATE TRIGGER hopper_jobs_tags_update AFTER UPDATE OF tags, id ON hopper_jobs
WHEN OLD.tags IS NOT NEW.tags OR OLD.id IS NOT NEW.id
BEGIN
  DELETE FROM hopper_job_tags
  WHERE OLD.tags IS NOT NULL AND id = OLD.id AND tag IN (SELECT value FROM json_each(OLD.tags) WHERE type = 'text');
  INSERT OR IGNORE INTO hopper_job_tags (tag, id)
  SELECT value, NEW.id FROM json_each(coalesce(NEW.tags, '[]')) WHERE type = 'text' AND value <> '';
END;

CREATE TRIGGER hopper_job_history_tags_insert AFTER INSERT ON hopper_job_history
WHEN NEW.tags IS NOT NULL
BEGIN
  INSERT OR IGNORE INTO hopper_job_history_tags (tag, id)
  SELECT value, NEW.id FROM json_each(NEW.tags) WHERE type = 'text' AND value <> '';
END;

CREATE TRIGGER hopper_job_history_tags_delete AFTER DELETE ON hopper_job_history
WHEN OLD.tags IS NOT NULL
BEGIN
  DELETE FROM hopper_job_history_tags
  WHERE id = OLD.id AND tag IN (SELECT value FROM json_each(OLD.tags) WHERE type = 'text');
END;

CREATE TRIGGER hopper_job_history_tags_update AFTER UPDATE OF tags, id ON hopper_job_history
WHEN OLD.tags IS NOT NEW.tags OR OLD.id IS NOT NEW.id
BEGIN
  DELETE FROM hopper_job_history_tags
  WHERE OLD.tags IS NOT NULL AND id = OLD.id AND tag IN (SELECT value FROM json_each(OLD.tags) WHERE type = 'text');
  INSERT OR IGNORE INTO hopper_job_history_tags (tag, id)
  SELECT value, NEW.id FROM json_each(coalesce(NEW.tags, '[]')) WHERE type = 'text' AND value <> '';
END;
