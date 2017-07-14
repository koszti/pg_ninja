CREATE OR REPLACE FUNCTION sch_ninja.fn_refresh_parts() 
RETURNS VOID as 
$BODY$
DECLARE
    t_sql text;
    r_tables record;
BEGIN
    FOR r_tables IN SELECT unnest(v_log_table) as v_log_table FROM sch_ninja.t_sources
    LOOP
        RAISE DEBUG 'CREATING TABLE %', r_tables.v_log_table;
        t_sql:=format('
			CREATE TABLE IF NOT EXISTS sch_ninja.%I
			(
			CONSTRAINT pk_%s PRIMARY KEY (i_id_event),
			  CONSTRAINT fk_%s FOREIGN KEY (i_id_batch) 
				REFERENCES  sch_ninja.t_replica_batch (i_id_batch)
			ON UPDATE RESTRICT ON DELETE CASCADE
			)
			INHERITS (sch_ninja.t_log_replica)
			;',
                        r_tables.v_log_table,
                        r_tables.v_log_table,
                        r_tables.v_log_table
                );
        EXECUTE t_sql;
	t_sql:=format('
			CREATE INDEX IF NOT EXISTS idx_id_batch_%s 
			ON sch_ninja.%I (i_id_batch)
			;',
			r_tables.v_log_table,
                        r_tables.v_log_table
		);
	EXECUTE t_sql;
    END LOOP;
END
$BODY$
LANGUAGE plpgsql 
;

SELECT sch_ninja.fn_refresh_parts() ;
