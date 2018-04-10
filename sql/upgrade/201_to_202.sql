-- upgrade catalogue script 2.0.1 to 2.0.2

ALTER TABLE sch_ninja.t_sources
	ADD COLUMN b_maintenance boolean NOT NULL DEFAULT False;

