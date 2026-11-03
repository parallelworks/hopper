-- hopper schema v24: a request that the leader give up its term.
--
-- hopper_leader.resign_client_id names the client a resign request
-- (Client.LeaderResign) asked to give the lease up: the client that held
-- it, live, when the request was made. The holder's renewal reports a
-- request that names it, and the holder then ends its term and deletes
-- the row, as on a graceful stop. A client that takes the lease over
-- clears it; one of the previous release does not, but it never reads the
-- column, and a request names its holder only, so it cannot ask a later
-- holder to resign. Adding a nullable column with no default rewrites
-- nothing.

ALTER TABLE hopper_leader ADD COLUMN resign_client_id bigint;
