-- hopper schema v23: any-of groups of workflow edges.
--
-- A workflow step can wait for one job of a group to complete rather than
-- for every job to finish (StepOpts.AfterAny). hopper_job_deps.any_of
-- numbers the dependent's group an edge is in, NULL for an ordinary edge,
-- and a group's edges store on_failure as 'any_cancel' or 'any_ignore',
-- never 'cancel', which the previous release's finalize would cancel the
-- dependent by as soon as one job of the group failed. The finalize of a
-- job with a dependent in a group records its final state in
-- hopper_job_results, as for a dependent that reads it; it reads every
-- dependent of a job by hopper_job_deps_depends_on, so the edges need no
-- index of their own.

ALTER TABLE hopper_job_deps ADD COLUMN any_of INTEGER;
