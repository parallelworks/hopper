-- hopper schema v24: a request that the leader give up its term.
--
-- hopper_leader.resign_client_id names the client a resign request
-- (Client.LeaderResign) asked to give the lease up: the client that held
-- it, live, when the request was made. The holder's renewal reports a
-- request that names it, and the holder then ends its term and deletes
-- the row. A request names its holder only, so it cannot ask a later
-- holder to resign.

ALTER TABLE hopper_leader ADD COLUMN resign_client_id INTEGER;
