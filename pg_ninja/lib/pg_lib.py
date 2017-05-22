import psycopg2
import os
import sys
import json
import datetime
import decimal
import time
import base64
import io

class pg_encoder(json.JSONEncoder):
		def default(self, obj):
			if isinstance(obj, datetime.time) or isinstance(obj, datetime.datetime) or  isinstance(obj, datetime.date) or isinstance(obj, decimal.Decimal) or isinstance(obj, datetime.timedelta):
				return str(obj)
			return json.JSONEncoder.default(self, obj)

class pg_connection:
	def __init__(self, global_config ):
		self.global_conf=global_config
		self.pg_conn=self.global_conf.pg_conn
		self.pg_database=self.global_conf.pg_database
		if self.global_conf.schema_clear:
			self.dest_schema=self.global_conf.schema_clear
		else:
			self.dest_schema=self.global_conf.my_database
		if self.global_conf.schema_obf:
			self.schema_obf=self.global_conf.schema_obf
		else:
			self.schema_obf=self.dest_schema+"_obf"
		self.pg_connection=None
		self.pg_cursor=None
		self.pg_charset=self.global_conf.pg_charset
		
		
	
	def connect_db(self, destination_schema=None):
		pg_pars=dict(self.pg_conn.items()+ {'dbname':self.pg_database}.items())
		strconn="dbname=%(dbname)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s"  % pg_pars
		self.pgsql_conn = psycopg2.connect(strconn)
		self.pgsql_conn .set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		self.pgsql_conn .set_client_encoding(self.pg_charset)
		self.pgsql_cur=self.pgsql_conn .cursor()
		if destination_schema:
			self.dest_schema=destination_schema
			self.schema_obf=None
			#self.tmp_schema=self.dest_schema+"_tmp"
		
	
	def disconnect_db(self):
		self.pgsql_conn.close()
		

class pg_engine:
	def __init__(self, global_config, table_metadata,  logger, sql_dir='sql/'):
		self.lst_yes = global_config.lst_yes
		self.logger=logger
		self.sql_dir=sql_dir
		self.idx_sequence=0
		self.skip_view=global_config.skip_view
		self.pg_conn=pg_connection(global_config)
		self.pg_conn.connect_db()
		self.table_metadata=table_metadata
		self.type_dictionary={
			'integer':'integer',
			'mediumint':'bigint',
			'tinyint':'integer',
			'smallint':'integer',
			'int':'integer',
			'bigint':'bigint',
			'varchar':'character varying',
			'text':'text',
			'char':'character',
			'datetime':'timestamp without time zone',
			'date':'date',
			'time':'time without time zone',
			'timestamp':'timestamp without time zone',
			'tinytext':'text',
			'mediumtext':'text',
			'longtext':'text',
			'tinyblob':'bytea',
			'mediumblob':'bytea',
			'longblob':'bytea',
			'blob':'bytea', 
			'binary':'bytea', 
			'decimal':'numeric', 
			'double':'double precision', 
			'double precision':'double precision', 
			'float':'float', 
			'bit':'integer', 
			'year':'integer', 
			'enum':'enum', 
			'set':'text', 
			'json':'text'
		}
		self.table_ddl={}
		self.idx_ddl={}
		self.type_ddl={}
		self.pg_charset=self.pg_conn.pg_charset
		self.batch_retention = global_config.batch_retention
		self.cat_version='0.10'
		self.cat_sql=[
			{'version':'base','script': 'create_schema.sql'}, 
			{'version':'0.8','script': 'upgrade/cat_0.8.sql'}, 
			{'version':'0.9','script': 'upgrade/cat_0.9.sql'}, 
			{'version':'0.10','script': 'upgrade/cat_0.10.sql'}, 
		]
		cat_version=self.get_schema_version()
		num_schema=(self.check_service_schema())[0]
		if cat_version!=self.cat_version and int(num_schema)>0:
			self.upgrade_service_schema()
		self.table_limit = ['*']
	
	
	def drop_obf_rel(self, relname, type):
		sql_unobf = """ DROP %s IF EXISTS "%s"."%s" CASCADE; """ % (type, self.pg_conn.schema_obf, relname)
		self.pg_conn.pgsql_cur.execute(sql_unobf)	
	
	def clear_obfuscation_reindex(self):
		self.logger.info("clearing existing idx definition for schema %s"  % (self.pg_conn.schema_obf))
		sql_del="""DELETE FROM sch_chameleon.t_rebuild_idx;"""
		self.pg_conn.pgsql_cur.execute(sql_del)	
	
	def drop_null_obf(self):
		self.logger.info("dropping null constraints in schema %s"  % (self.pg_conn.schema_obf))
		sql_gen_drop="""
			WITH t_key as(
				SELECT
					sch.nspname schema_name,
					tab.relname table_name,
					att.attname column_name
				FROM
					pg_constraint con
					INNER JOIN pg_namespace sch
						ON sch.oid=connamespace
					INNER JOIN pg_attribute att
						ON
							att.attrelid=con.conrelid
						AND	att.attnum = any(con.conkey)
					INNER JOIN pg_class tab
						ON
							att.attrelid=tab.oid
				WHERE
					con.contype='p'
					and sch.nspname=%s
				)
				SELECT
					format(
							'ALTER TABLE %%I.%%I ALTER COLUMN %%I DROP NOT NULL;',
							table_schema,
							table_name,
							column_name
						) as drop_null
				FROM
					information_schema.columns col
				WHERE
						(
							table_schema,
							table_name,
							column_name
						) NOT IN
							(
								SELECT 
									schema_name,
									table_name,
									column_name 
								FROM 
									t_key
							)
					AND	table_schema=%s
					AND	Is_nullable='NO'
				;

		"""
		self.pg_conn.pgsql_cur.execute(sql_gen_drop, (self.obf_schema, self.obf_schema ))	
		null_cols=self.pg_conn.pgsql_cur.fetchall()
		for null_col in null_cols:
			try:
				self.pg_conn.pgsql_cur.execute(null_col [0])
			except psycopg2.Error as e:
				self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
				self.logger.error(null_col [0])
		
		
	def sync_obfuscation(self, obfdic, clean_idx=False):
		
		if clean_idx:
			self.clear_obfuscation_reindex()
		else:
			drp_msg = 'Do you want to clean the existing index definitions in t_rebuild_idx?.\n YES/No\n' 
			if sys.version_info[0] == 3:
				drop_idx = input(drp_msg)
			else:
				drop_idx = raw_input(drp_msg)
		if drop_idx == 'YES':
			self.clear_obfuscation_reindex()
		elif drop_idx in self.lst_yes or len(drop_idx) == 0:
			print('Please type YES all uppercase to confirm')
			sys.exit()
		self.drop_null_obf()
		table_limit = ''
		if self.table_limit[0] != '*':
			table_limit = self.pg_conn.pgsql_cur.mogrify(""" AND table_name IN  (SELECT unnest(%s))""",(self.table_limit, )).decode()
			table_limit_pk = self.pg_conn.pgsql_cur.mogrify(""" AND tab.relname IN  (SELECT unnest(%s))""",(self.table_limit, )).decode()
		
		
		sql_get_clear="""
			SELECT 
				table_schema,
				table_name 
				
			FROM  
				information_schema.tables 
			WHERE 
					table_name NOT IN  
						(
							SELECT 
								unnest(%s)
						) 
				AND table_schema=%s
				AND table_type='BASE TABLE'
				""" + table_limit + """
			;
		"""
		sql_get_obf="""
				SELECT 
					table_name 
				FROM
					information_schema.tables
				WHERE
					table_name IN  (SELECT  unnest(%s)) 
					AND table_schema=%s
					AND table_type='BASE TABLE'
					""" + table_limit + """
			EXCEPT
				SELECT 
					table_name FROM
				information_schema.tables
				WHERE
						table_name IN  (SELECT unnest(%s)) 
					AND table_schema=%s
					AND table_type='BASE TABLE'
					""" + table_limit + """
		;
		"""
		obf_list = []
		for obf_table in  obfdic:
			obf_list.append(obf_table)
		
		self.logger.info("saving index and key definitions for tables in schema %s"  % (self.obf_schema))
		
		sql_idx_def="""
			
			INSERT INTO sch_chameleon.t_rebuild_idx
				(
					v_schema_name,
					v_table_name,
					v_index_name,
					v_index_type,
					t_create,
					t_drop
				)
				SELECT
					table_schema,
					table_name,
					tabidx.relname,
					'index'::character varying(30),
					pg_get_indexdef(indexrelid,0,true)||';' as t_create,
					format('DROP INDEX %%I.%%I;',
						table_schema,
						tabidx.relname
					) AS t_drop
					
				FROM 
					pg_index idx
					INNER JOIN 
						(
							SELECT 
								format('%%I.%%I',table_schema,table_name)::regclass tabid,
								table_schema,
								TABLE_NAME
							FROM
								information_schema.tables tab
									
							WHERE
									table_name IN  (SELECT unnest(%s)) 
									AND table_schema=%s
								AND table_type='BASE TABLE'
								""" + table_limit + """
						) tab
					ON 
						tab.tabid=idx.indrelid
					INNER JOIN pg_class tabidx
					ON
						idx.indexrelid=tabidx.oid
				WHERE 
					not indisprimary
				ON CONFLICT DO NOTHING

				;
		"""
		self.pg_conn.pgsql_cur.execute(sql_idx_def, (obf_list, self.obf_schema))	
		sql_pkeys = """
							INSERT INTO sch_chameleon.t_rebuild_idx
							(
								v_schema_name,
								v_table_name,
								v_index_name,
								v_index_type,
								t_create,
								t_drop
							)
							SELECT
								sch.nspname,
								tab.relname,
								con.conname,
								'primary'::character varying(30),
								format('ALTER TABLE %%I.%%I ADD CONSTRAINT %%I %%s;',
									sch.nspname,
									tab.relname,
									con.conname,
									pg_get_constraintdef(con.oid)
								) AS t_create,
								format('ALTER TABLE %%I.%%I DROP CONSTRAINT %%I CASCADE;',
									sch.nspname,
									tab.relname,
									con.conname
								) AS t_drop
								
							FROM 
								pg_constraint con
								INNER JOIN
								pg_class tab
								ON
									tab.oid=con.conrelid
								INNER JOIN 
								pg_namespace sch
								ON
									sch.oid=con.connamespace
							WHERE
									con.contype='p'
								AND tab.relname IN  (SELECT unnest(%s)) 
								AND sch.nspname=%s
								""" + table_limit_pk + """
								
							ON CONFLICT DO NOTHING
							;
					"""
		self.pg_conn.pgsql_cur.execute(sql_pkeys, (obf_list, self.obf_schema))	
		
		self.logger.info("finding tables no longer obfuscated...")
		self.pg_conn.pgsql_cur.execute(sql_get_clear, (obf_list, self.obf_schema))	
		tab_clear=self.pg_conn.pgsql_cur.fetchall()
		self.logger.info("finding tables requiring obfuscation...")
		self.pg_conn.pgsql_cur.execute(sql_get_obf, (obf_list, self.dest_schema, obf_list, self.obf_schema))	
		tab_obf=self.pg_conn.pgsql_cur.fetchall()
		for tab in tab_clear:
			self.logger.info("dropping table %s from the schema %s " % (tab[1], tab[0]))
			self.drop_obf_rel(tab[1], "TABLE")
		
		for tab in tab_obf:
			self.logger.info("creating table %s in schema %s " % (tab[0], self.obf_schema))
			self.drop_obf_rel(tab[0], "VIEW")
			self.create_obf_child(tab[0])
			self.sync_obf_table(tab[0], obfdic[tab[0]])
			sql_pk = """SELECT 
								format('ALTER TABLE %%I.%%I ADD CONSTRAINT %%I %%s;',
											%s,
											tab.relname,
											pk.conname,
											pg_get_constraintdef(pk.oid)
										) AS t_create
							FROM 
								pg_constraint pk
								INNER JOIN pg_namespace sch 
								ON 
									sch.oid=pk.connamespace
								INNER JOIN pg_class tab 
								ON
									tab.oid=pk.conrelid
							WHERE
									sch.nspname=%s
								AND pk.contype='p'
								AND tab.relname=%s
							; """
			self.pg_conn.pgsql_cur.execute(sql_pk, (self.obf_schema, self.dest_schema, tab[0]))	
			tab_pk=self.pg_conn.pgsql_cur.fetchone()
			self.pg_conn.pgsql_cur.execute(tab_pk[0])
		for tab in obfdic:
			if self.table_limit == '*' or tab in self.table_limit:
				self.sync_obf_table(tab, obfdic[tab])
		
		self.create_views(obfdic)
		
		
	def sync_obf_table(self, tab, obfdata):
		self.logger.info("dropping indices and pkey on table %s in schema %s " % (tab, self.obf_schema))	
		sql_get_drop="""SELECT 
						t_drop
					FROM 
						sch_chameleon.t_rebuild_idx  
					WHERE
						v_table_name=%s
						AND v_schema_name=%s
					; """
		self.pg_conn.pgsql_cur.execute(sql_get_drop, (tab, self.obf_schema))	
		drop_idx=self.pg_conn.pgsql_cur.fetchall()
		for drop_stat in drop_idx:
			self.pg_conn.pgsql_cur.execute(drop_stat[0])
		
		self.logger.info("syncronising data for table %s in schema %s " % (tab, self.obf_schema))	
		self.copy_obf_data(tab, obfdata)
		self.logger.info("creating indices and pkey on table %s in schema %s " % (tab, self.obf_schema))	
		sql_get_create="""SELECT 
						t_create
					FROM 
						sch_chameleon.t_rebuild_idx  
					WHERE
						v_table_name=%s
						AND v_schema_name=%s
					; """
		self.pg_conn.pgsql_cur.execute(sql_get_create, (tab, self.obf_schema))	
		create_idx=self.pg_conn.pgsql_cur.fetchall()
		for create_stat in create_idx:
			try:
				self.pg_conn.pgsql_cur.execute(create_stat [0])
			except psycopg2.Error as e:
				self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
				self.logger.error(create_stat [0])
		
		
	def create_obf_child(self, table):
		sql_check="""SELECT 
									count(*) 
								FROM
									information_schema.tables 
								WHERE 
													table_schema=%s 
										AND 	table_name=%s;
						"""
		self.pg_conn.pgsql_cur.execute(sql_check, (self.dest_schema, table))	
		tab_count=self.pg_conn.pgsql_cur.fetchone()
		if tab_count[0]>0:
			sql_child="""
				DROP TABLE  IF EXISTS \"""" + self.obf_schema + """\".\"""" + table + """\" ; 
				CREATE TABLE \"""" + self.obf_schema + """\".\"""" + table + """\"  
				(LIKE \"""" + self.dest_schema + """\".\"""" + table + """\")
				;
			"""
			self.pg_conn.pgsql_cur.execute(sql_child)
			self.alter_obf_fields(table)
			return True
		else:
			return False
	
	def alter_obf_fields(self, table):
		""" """
		sql_alter="""
			WITH 
				t_filter AS
					(
						SELECT
						%s::text AS table_schema,
						%s::text AS table_name
					),
					t_key AS
					(
						SELECT 
						column_name 
						FROM
							information_schema.key_column_usage keycol 
						INNER JOIN t_filter fil
						ON 
							keycol.table_schema=fil.table_schema
							AND keycol.table_name=fil.table_name
					)

				SELECT 
					format('ALTER TABLE %%I.%%I ALTER COLUMN %%I TYPE text ;',
					col.table_schema,
					col.table_name,
					col.column_name
					) AS alter_table
				FROM
					information_schema.columns col
					INNER JOIN t_filter fil
					    ON
						col.table_schema=fil.table_schema
					    AND col.table_name=fil.table_name
				WHERE 
				     column_name NOT IN (
							SELECT 
								column_name 
							    FROM
								t_key
							       )
					 AND data_type = 'character varying'
				UNION ALL

				SELECT 
				    format('ALTER TABLE %%I.%%I ALTER COLUMN %%I DROP NOT NULL;',
					col.table_schema,
					col.table_name,
					col.column_name
					) AS alter_table
				FROM
					information_schema.columns col
					INNER JOIN t_filter fil
					    ON 
						col.table_schema=fil.table_schema
					    AND col.table_name=fil.table_name
				WHERE 
				     column_name NOT IN (
							    SELECT 
								column_name 
							    FROM
								t_key
							       )
					 AND is_nullable = 'NO'
				;
		"""
		self.pg_conn.pgsql_cur.execute(sql_alter, (self.obf_schema, table, ))
		alter_stats = self.pg_conn.pgsql_cur.fetchall()
		for alter in alter_stats:
			self.pg_conn.pgsql_cur.execute(alter[0])
	
	def copy_obf_data(self, table, obfdic):
		sql_crypto="SELECT count(*) FROM pg_catalog.pg_extension where extname='pgcrypto';"
		self.pg_conn.pgsql_cur.execute(sql_crypto)
		pg_crypto=self.pg_conn.pgsql_cur.fetchone()
		if pg_crypto[0] == 0:
			self.logger.info("extension pgcrypto missing on database. falling back to md5 obfuscation")
		col_list=[]
		sql_cols=""" 
					SELECT
						column_name,
						CASE
							WHEN 
								character_maximum_length IS NOT NULL 
							THEN
								format('::%%s(%%s)',data_type,character_maximum_length) 
							ELSE
								format('::%%s',data_type)
						END AS data_cast
					FROM
						information_schema.COLUMNS
					WHERE 
								table_schema=%s
						AND table_name=%s
					ORDER BY 
						ordinal_position 
					;
			"""
		self.pg_conn.pgsql_cur.execute(sql_cols, (self.pg_conn.dest_schema,table ))
		columns=self.pg_conn.pgsql_cur.fetchall()
		for column in columns:
			try:
				obfdata=obfdic[column[0]]
				if obfdata["mode"]=="normal":
					if pg_crypto[0] == 0:
						col_list.append("(substr(\"%s\"::text, %s , %s)||md5(\"%s\"))%s" %(column[0], obfdata["nonhash_start"], obfdata["nonhash_length"], column[0],  column[1]))
					else:
						col_list.append("(substr(\"%s\"::text, %s , %s)||encode(public.digest(\"%s\",'sha256'),'hex'))%s" %(column[0], obfdata["nonhash_start"], obfdata["nonhash_length"], column[0],  column[1]))

				elif obfdata["mode"]=="date":
					col_list.append("to_char(\"%s\"::date,'YYYY-01-01')::date" % (column[0]))
				elif obfdata["mode"] == "numeric":
					col_list.append("0%s" % (column[1]))
				elif obfdata["mode"] == "setnull":
					col_list.append("NULL%s" % (column[1]))
			except:
				col_list.append('"%s"'%(column[0], ))
				
		tab_exists=self.truncate_table(table, self.obf_schema)
		if tab_exists:
			sql_insert="""INSERT INTO  \"""" + self.obf_schema + """\".\"""" + table + """\"  SELECT """ + ','.join(col_list) + """ FROM  \"""" + self.pg_conn.dest_schema + """\".\"""" + table + """\" ;"""
			self.logger.debug("copying table: %s in obfuscated schema" % (table, ))
			self.pg_conn.pgsql_cur.execute(sql_insert)
			
	def create_views(self, obfdic):
		self.logger.info("creating views for tables not in obfuscation list")
		table_obf=[table for table in obfdic]
		if self.skip_view:
			table_obf = table_obf + self.skip_view
		
		sql_create="""
					SELECT 
							format('CREATE OR REPLACE VIEW %%I.%%I AS SELECT * FROM %%I.%%I ;',
							%s,
							table_name,
							table_schema,
							table_name
							),
							table_name,
							table_schema
					FROM
						information_schema.TABLES 
					WHERE 
					table_schema=%s
					AND table_name not in (SELECT unnest(%s))
					;
				"""
		self.pg_conn.pgsql_cur.execute(sql_create, (self.obf_schema, self.dest_schema, table_obf, ))
		create_views=self.pg_conn.pgsql_cur.fetchall()
		for statement in create_views:
			try:
				self.pg_conn.pgsql_cur.execute(statement[0])
			except psycopg2.Error as e:
				if e.pgcode == '42809':
					self.logger.info("replacing table %s in schema %s with a view. old table is renamed to %s_bak" % (self.obf_schema, statement[2],   statement[1]))
					sql_rename="""ALTER TABLE "%s"."%s" RENAME TO "%s_bak" ;""" % (self.obf_schema,  statement[1], statement[1])
					self.pg_conn.pgsql_cur.execute(sql_rename)
					self.pg_conn.pgsql_cur.execute(statement[0])
				else:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(statement[0])


	def copy_obfuscated(self, obfdic, tables_limit):
		table_obf={}
		if tables_limit:
			for table in tables_limit:
				try:
					table_obf[table]=obfdic[table]
				except:
					pass
		else:
			table_obf=obfdic
		for table in table_obf:
			if self.create_obf_child(table):
				self.copy_obf_data(table, table_obf[table])
	
	def create_schema(self):
		
		if self.obf_schema:
			sql_schema=" CREATE SCHEMA IF NOT EXISTS "+self.obf_schema+";"
			self.pg_conn.pgsql_cur.execute(sql_schema)
		sql_schema=" CREATE SCHEMA IF NOT EXISTS "+self.dest_schema+";"
		sql_path=" SET search_path="+self.dest_schema+";"
		self.pg_conn.pgsql_cur.execute(sql_schema)
		self.pg_conn.pgsql_cur.execute(sql_path)
	
	def store_table(self, table_name):
		table_data=self.table_metadata[table_name]
		for index in table_data["indices"]:
			if index["index_name"]=="PRIMARY":
				sql_insert=""" INSERT INTO sch_chameleon.t_replica_tables 
										(
											i_id_source,
											v_table_name,
											v_schema_name,
											v_table_pkey
										)
										VALUES (
														%s,
														%s,
														%s,
														ARRAY[%s]
													)
										ON CONFLICT (i_id_source,v_table_name,v_schema_name)
											DO UPDATE 
												SET v_table_pkey=EXCLUDED.v_table_pkey
										;
								"""
				self.pg_conn.pgsql_cur.execute(sql_insert, (self.i_id_source, table_name, self.dest_schema, index["index_columns"].strip()))	
				self.pg_conn.pgsql_cur.execute(sql_insert, (self.i_id_source, table_name, self.obf_schema, index["index_columns"].strip()))	
	
		
	
	def unregister_table(self, table_name):
		self.logger.info("unregistering table %s from the replica catalog" % (table_name,))
		sql_delete=""" DELETE FROM sch_chameleon.t_replica_tables 
									WHERE
											v_table_name=%s
										AND	v_schema_name=%s
								RETURNING i_id_table
								;
						"""
		self.pg_conn.pgsql_cur.execute(sql_delete, (table_name, self.dest_schema))	
		removed_id=self.pg_conn.pgsql_cur.fetchone()
		table_id=removed_id[0]
		self.logger.info("renaming table %s to %s_%s" % (table_name, table_name, table_id))
		sql_rename="""ALTER TABLE IF EXISTS "%s"."%s" rename to "%s_%s"; """ % (self.dest_schema, table_name, table_name, table_id)
		self.logger.debug(sql_rename)
		self.pg_conn.pgsql_cur.execute(sql_rename)	
	
	def create_tables(self, drop_tables=False, store_tables=True):
			for table in self.table_ddl:
				if drop_tables:
					sql_drop_clear='DROP TABLE IF EXISTS  "%s"."%s" CASCADE ;' % (self.pg_conn.dest_schema, table,)
					sql_drop_obf='DROP TABLE IF EXISTS  "%s"."%s" CASCADE ;' % (self.obf_schema, table,)
					self.pg_conn.pgsql_cur.execute(sql_drop_clear)
					self.pg_conn.pgsql_cur.execute(sql_drop_obf)
				try:
					ddl_enum=self.type_ddl[table]
					for sql_type in ddl_enum:
						self.pg_conn.pgsql_cur.execute(sql_type)
				except:
					pass
				sql_create=self.table_ddl[table]
				try:
					self.pg_conn.pgsql_cur.execute(sql_create)
				except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(sql_create)
				self.logger.debug('Storing table %s in t_replica_tables' % (table, ))
				if store_tables:
					self.store_table(table)
	
	def create_indices(self):
		self.logger.info("creating the indices")
		for index in self.idx_ddl:
			idx_ddl= self.idx_ddl[index]
			for sql_idx in idx_ddl:
				self.pg_conn.pgsql_cur.execute(sql_idx)
	
	def reset_sequences(self, destination_schema):
		""" method to reset the sequences to the max value available in table """
		self.logger.info("resetting the sequences in schema %s" % destination_schema)
		sql_gen_reset=""" SELECT 
													format('SELECT setval(%%L::regclass,(select max(id) FROM %%I.%%I));',
														replace(replace(column_default,'nextval(''',''),'''::regclass)',''),
														table_schema,
														table_name
													)
									FROM 
										information_schema.columns
									WHERE 
											table_schema=%s
										AND column_default like 'nextval%%'
								;"""
		self.pg_conn.pgsql_cur.execute(sql_gen_reset, (destination_schema, ))
		results=self.pg_conn.pgsql_cur.fetchall()
		try:
			for statement in results[0]:
				self.pg_conn.pgsql_cur.execute(statement)
		except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(statement)
		except:
			pass
			
	def copy_data(self, table,  csv_file,  my_tables={}):
		column_copy=[]
		for column in my_tables[table]["columns"]:
			column_copy.append('"'+column["column_name"]+'"')
		sql_copy="COPY "+'"'+self.dest_schema+'"'+"."+'"'+table+'"'+" ("+','.join(column_copy)+") FROM STDIN WITH NULL 'NULL' CSV QUOTE '\"' DELIMITER',' ESCAPE '\"' ; "
		self.pg_conn.pgsql_cur.copy_expert(sql_copy,csv_file)
	
		
	def insert_data(self, table,  insert_data,  my_tables={}):
		column_copy=[]
		column_marker=[]
		
		for column in my_tables[table]["columns"]:
			column_copy.append('"'+column["column_name"]+'"')
			column_marker.append('%s')
		sql_head="INSERT INTO "+'"'+self.pg_conn.dest_schema+'"'+"."+'"'+table+'"'+" ("+','.join(column_copy)+") VALUES ("+','.join(column_marker)+");"
		for data_row in insert_data:
			column_values=[]
			for column in my_tables[table]["columns"]:
				column_values.append(data_row[column["column_name"]])
			try:
				self.pg_conn.pgsql_cur.execute(sql_head,column_values)	
			except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(self.pg_conn.pgsql_cur.mogrify(sql_head,column_values))
				
	def build_tab_ddl(self):
		""" the function iterates over the list l_tables and builds a new list with the statements for tables"""
		
		for table_name in self.table_metadata:
			table=self.table_metadata[table_name]
			columns=table["columns"]
			
			ddl_head="CREATE TABLE "+'"'+table["name"]+'" ('
			ddl_tail=");"
			ddl_columns=[]
			ddl_enum=[]
			for column in columns:
				if column["is_nullable"]=="NO":
					col_is_null="NOT NULL"
				else:
					col_is_null="NULL"
				column_type=self.type_dictionary[column["data_type"]]
				if column_type=="enum":
					enum_type="enum_"+table["name"]+"_"+column["column_name"]
					sql_drop_enum='DROP TYPE IF EXISTS '+enum_type+' CASCADE;'
					sql_create_enum="CREATE TYPE "+enum_type+" AS ENUM "+column["enum_list"]+";"
					ddl_enum.append(sql_drop_enum)
					ddl_enum.append(sql_create_enum)
					column_type=enum_type
				if column_type=="character varying" or column_type=="character":
					column_type=column_type+"("+str(column["character_maximum_length"])+")"
				if column_type=='bit' or column_type=='float' or column_type=='numeric':
					column_type=column_type+"("+str(column["numeric_precision"])+")"
				if column["extra"]=="auto_increment":
					column_type="bigserial"
				ddl_columns.append('"'+column["column_name"]+'" '+column_type+" "+col_is_null )
			def_columns=str(',').join(ddl_columns)
			self.type_ddl[table["name"]]=ddl_enum
			self.table_ddl[table["name"]]=ddl_head+def_columns+ddl_tail
		
	
	def build_idx_ddl(self, obfdic={}):
		table_obf=[table for table in obfdic]
		
		""" the function iterates over the list l_pkeys and builds a new list with the statements for pkeys """
		for table_name in self.table_metadata:
			table=self.table_metadata[table_name]
			
			table_name=table["name"]
			indices=table["indices"]
			table_idx=[]
			for index in indices:
				indx=index["index_name"]
				index_columns=index["index_columns"]
				non_unique=index["non_unique"]
				if indx=='PRIMARY':
					pkey_name="pk_"+table_name[0:20]+"_"+str(self.idx_sequence)
					pkey_def='ALTER TABLE "'+table_name+'" ADD CONSTRAINT "'+pkey_name+'" PRIMARY KEY ('+index_columns+') ;'
					table_idx.append(pkey_def)
					if table_name in table_obf:
						pkey_def='ALTER TABLE "'+self.obf_schema+'"."'+table_name+'" ADD CONSTRAINT "'+pkey_name+'" PRIMARY KEY ('+index_columns+') ;'
						table_idx.append(pkey_def)
				else:
					if non_unique==0:
						unique_key='UNIQUE'
					else:
						unique_key=''
					index_name='"idx_'+indx[0:20]+table_name[0:20]+"_"+str(self.idx_sequence)+'"'
					idx_def='CREATE '+unique_key+' INDEX '+ index_name+' ON "'+table_name+'" ('+index_columns+');'
					table_idx.append(idx_def)
					if table_name in table_obf:
						idx_def='CREATE '+unique_key+' INDEX '+ index_name+' ON "'+self.obf_schema+'"."'+table_name+'" ('+index_columns+');'
						table_idx.append(idx_def)
						
				self.idx_sequence+=1
					
			self.idx_ddl[table_name]=table_idx
			
	def get_schema_version(self):
		"""
			Gets the service schema version.
		"""
		sql_check="""
			SELECT 
				t_version
			FROM 
				sch_chameleon.v_version 
			;
		"""
		try:
			self.pg_conn.pgsql_cur.execute(sql_check)
			value_check=self.pg_conn.pgsql_cur.fetchone()
			cat_version=value_check[0]
		except:
			cat_version='base'
		return cat_version
		
	def upgrade_service_schema(self):
		"""
			Upgrade the service schema to the latest version using the upgrade files
		"""
		
		self.logger.info("Upgrading the service schema")
		install_script=False
		cat_version=self.get_schema_version()
		for install in self.cat_sql:
			script_ver=install["version"]
			script_schema=install["script"]
			self.logger.info("script schema %s, detected schema version %s - target version: %s - install_script:%s " % (script_ver, cat_version, self.cat_version,  install_script))
			if install_script==True:
				self.logger.info("Installing file version %s" % (script_ver, ))
				file_schema=open(self.sql_dir+script_schema, 'rb')
				sql_schema=file_schema.read()
				file_schema.close()
				self.pg_conn.pgsql_cur.execute(sql_schema)
				if script_ver=='0.9':
						sql_update="""
							UPDATE sch_chameleon.t_sources
							SET
								t_dest_schema=%s,
								t_obf_schema=%s
							WHERE i_id_source=(
												SELECT 
													i_id_source
												FROM
													sch_chameleon.t_sources
												WHERE
													t_source='default'
													AND t_dest_schema='default'
													AND t_obf_schema='default'
											)
							;
						"""
						self.pg_conn.pgsql_cur.execute(sql_update, (self.pg_conn.dest_schema,self.pg_conn.schema_obf ))
			if script_ver==cat_version and not install_script:
				self.logger.info("enabling install script")
				install_script=True
				
	def check_service_schema(self):
		sql_check="""
								SELECT 
									count(*)
								FROM 
									information_schema.schemata  
								WHERE 
									schema_name='sch_chameleon'
						"""
			
		self.pg_conn.pgsql_cur.execute(sql_check)
		num_schema=self.pg_conn.pgsql_cur.fetchone()
		return num_schema
	
	def create_service_schema(self):
		
		num_schema=self.check_service_schema()
		if num_schema[0]==0:
			for install in self.cat_sql:
				script_ver=install["version"]
				script_schema=install["script"]
				if script_ver=='base':
					self.logger.info("Installing service schema %s" % (script_ver, ))
					file_schema=open(self.sql_dir+script_schema, 'rb')
					sql_schema=file_schema.read()
					file_schema.close()
					self.pg_conn.pgsql_cur.execute(sql_schema)
		else:
			self.logger.error("The service schema is already created")
			
		
	def drop_service_schema(self):
		file_schema=open(self.sql_dir+"drop_schema.sql", 'rb')
		sql_schema=file_schema.read()
		file_schema.close()
		self.pg_conn.pgsql_cur.execute(sql_schema)
	
	def save_master_status(self, master_status, cleanup=False):
		next_batch_id=None
		sql_tab_log=""" 
							SELECT 
								CASE
									WHEN v_log_table='t_log_replica_2'
									THEN 
										't_log_replica_1'
									ELSE
										't_log_replica_2'
								END AS v_log_table
							FROM
								(
									(
									SELECT
											v_log_table,
											ts_created
											
									FROM
											sch_chameleon.t_replica_batch
									WHERE 
										i_id_source=%s
									)
									UNION ALL
									(
										SELECT
											't_log_replica_2'  AS v_log_table,
											'1970-01-01'::timestamp as ts_created
									)
									ORDER BY 
										ts_created DESC
									LIMIT 1
								) tab
						;
					"""
		self.pg_conn.pgsql_cur.execute(sql_tab_log, (self.i_id_source, ))
		results = self.pg_conn.pgsql_cur.fetchone()
		table_file = results[0]
		master_data = master_status[0]
		binlog_name = master_data["File"]
		binlog_position = master_data["Position"]
		try:
			event_time = datetime.datetime.fromtimestamp(master_data["Time"]).isoformat()
		except:
			event_time  = None
		self.logger.debug("master data: table file %s, log name: %s, log position: %s " % (table_file, binlog_name, binlog_position))
		sql_master="""
							INSERT INTO sch_chameleon.t_replica_batch
															(
																i_id_source,
																t_binlog_name, 
																i_binlog_position,
																v_log_table
															)
												VALUES (
																%s,
																%s,
																%s,
																%s
															)
							--ON CONFLICT DO NOTHING
							RETURNING i_id_batch
							;
						"""
						
		sql_event="""UPDATE sch_chameleon.t_sources 
					SET 
						ts_last_event=%s 
					WHERE 
						i_id_source=%s; 
						"""
		self.logger.info("saving master data id source: %s log file: %s  log position:%s Last event: %s" % (self.i_id_source, binlog_name, binlog_position, event_time))
		
		
		try:
			if cleanup:
				self.logger.info("cleaning not replayed batches for source %s", self.i_id_source)
				sql_cleanup=""" DELETE FROM sch_chameleon.t_replica_batch WHERE i_id_source=%s AND NOT b_replayed; """
				self.pg_conn.pgsql_cur.execute(sql_cleanup, (self.i_id_source, ))
			self.pg_conn.pgsql_cur.execute(sql_master, (self.i_id_source, binlog_name, binlog_position, table_file))
			results=self.pg_conn.pgsql_cur.fetchone()
			next_batch_id=results[0]
		except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(self.pg_conn.pgsql_cur.mogrify(sql_master, (self.i_id_source, binlog_name, binlog_position, table_file)))
		try:
			self.pg_conn.pgsql_cur.execute(sql_event, (event_time, self.i_id_source, ))
			
		except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.pg_conn.pgsql_cur.mogrify(sql_event, (event_time, self.i_id_source, ))
		
		return next_batch_id
		
		
	def get_batch_data(self):
		sql_batch="""WITH t_created AS
						(
							SELECT 
								max(ts_created) AS ts_created
							FROM 
								sch_chameleon.t_replica_batch  
							WHERE 
											NOT b_processed
								AND 	NOT b_replayed
								AND		i_id_source=%s
						)
					UPDATE sch_chameleon.t_replica_batch
						SET b_started=True
						FROM 
							t_created
						WHERE
							t_replica_batch.ts_created=t_created.ts_created
					RETURNING
						i_id_batch,
						t_binlog_name,
						i_binlog_position ,
						v_log_table
					;
					"""
		self.pg_conn.pgsql_cur.execute(sql_batch, (self.i_id_source, ))
		return self.pg_conn.pgsql_cur.fetchall()
	
	
	def save_discarded_row(self,row_data,batch_id):
		print(str(row_data))
		b64_row=base64.b64encode(str(row_data))
		sql_save="""INSERT INTO sch_chameleon.t_discarded_rows(
											i_id_batch, 
											t_row_data
											)
						VALUES (%s,%s);
						"""
		self.pg_conn.pgsql_cur.execute(sql_save,(batch_id,b64_row))
	
	def write_batch(self, group_insert):
		csv_file=io.StringIO()
		
		insert_list=[]
		for row_data in group_insert:
			global_data=row_data["global_data"]
			event_data=row_data["event_data"]
			event_update=row_data["event_update"]
			log_table=global_data["log_table"]
			insert_list.append(self.pg_conn.pgsql_cur.mogrify("%s,%s,%s,%s,%s,%s,%s,%s" ,  (
																	global_data["batch_id"], 
																	global_data["table"],  
																	global_data["schema"], 
																	global_data["action"], 
																	global_data["binlog"], 
																	global_data["logpos"], 
																	json.dumps(event_data, cls=pg_encoder), 
																	json.dumps(event_update, cls=pg_encoder)
																)
															)
														)
											
		csv_data=b"\n".join(insert_list ).decode()
		csv_file.write(csv_data)
		csv_file.seek(0)
		try:
			
			#self.pg_conn.pgsql_cur.execute(sql_insert)
			sql_copy="""COPY "sch_chameleon"."""+log_table+""" (
									i_id_batch, 
									v_table_name, 
									v_schema_name, 
									enm_binlog_event, 
									t_binlog_name, 
									i_binlog_position, 
									jsb_event_data,
									jsb_event_update
								) FROM STDIN WITH NULL 'NULL' CSV QUOTE '''' DELIMITER ',' ESCAPE '''' ; """
			self.pg_conn.pgsql_cur.copy_expert(sql_copy,csv_file)
		except psycopg2.Error as e:
			self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
			self.logger.error(csv_data)
			self.logger.error("fallback to inserts")
			self.insert_batch(group_insert)
	
	def insert_batch(self,group_insert):
		self.logger.debug("starting insert loop")
		for row_data in group_insert:
			global_data=row_data["global_data"]
			event_data=row_data["event_data"]
			event_update=row_data["event_update"]
			log_table=global_data["log_table"]
			sql_insert="""
				INSERT INTO sch_chameleon."""+log_table+"""
				(
					i_id_batch, 
					v_table_name, 
					v_schema_name, 
					enm_binlog_event, 
					t_binlog_name, 
					i_binlog_position, 
					jsb_event_data,
					jsb_event_update
				)
				VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
				;						
			"""
			try:
				self.pg_conn.pgsql_cur.execute(sql_insert,(
					global_data["batch_id"], 
					global_data["table"],  
					global_data["schema"], 
					global_data["action"], 
					global_data["binlog"], 
					global_data["logpos"], 
					json.dumps(event_data, cls=pg_encoder), 
					json.dumps(event_update, cls=pg_encoder))
				)
			except:
				self.logger.error("error when storing event data. saving the discarded row")
				self.save_discarded_row(row_data,global_data["batch_id"])
	

	def set_batch_processed(self, id_batch):
		self.logger.debug("updating batch %s to processed" % (id_batch, ))
		sql_update=""" UPDATE sch_chameleon.t_replica_batch
										SET
												b_processed=True,
												ts_processed=now()
								WHERE
										i_id_batch=%s
								;
							"""
		self.pg_conn.pgsql_cur.execute(sql_update, (id_batch, ))
		
	def process_batch(self, replica_batch_size):
		self.logger.debug("Replay batch in %s row chunks" % (replica_batch_size, ))
		batch_loop=True
		sql_process="""SELECT sch_chameleon.fn_process_batch(%s,%s);"""
		while batch_loop:
			self.pg_conn.pgsql_cur.execute(sql_process, (replica_batch_size, self.i_id_source))
			batch_result=self.pg_conn.pgsql_cur.fetchone()
			batch_loop=batch_result[0]
			self.logger.debug("Batch loop value %s" % (batch_loop))
		self.logger.debug("Cleaning replayed batches older than %s for source %s" % (self.batch_retention, self.i_id_source))
		sql_cleanup="""DELETE FROM 
									sch_chameleon.t_replica_batch
								WHERE
										b_started
									AND b_processed
									AND b_replayed
									AND now()-ts_replayed>%s::interval
									AND i_id_source=%s
									 """
		self.pg_conn.pgsql_cur.execute(sql_cleanup, (self.batch_retention, self.i_id_source))


	def build_alter_table(self, token):
		""" the function builds the alter table statement from the token idata"""
		alter_cmd=[]
		ddl_enum=[]
		query_cmd=token["command"]
		table_name=token["name"]
		for alter_dic in token["alter_cmd"]:
			if alter_dic["command"] == 'DROP':
				alter_cmd.append("%(command)s \"%(name)s\" CASCADE" % alter_dic)
			elif alter_dic["command"] == 'ADD':
				column_type=self.type_dictionary[alter_dic["type"]]
				if column_type=="enum":
					enum_name="enum_"+table_name+"_"+alter_dic["name"]
					column_type=enum_name
					sql_drop_enum='DROP TYPE IF EXISTS '+column_type+' CASCADE;'
					sql_create_enum="CREATE TYPE "+column_type+" AS ENUM ("+alter_dic["dimension"]+");"
					ddl_enum.append(sql_drop_enum)
					ddl_enum.append(sql_create_enum)
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				alter_cmd.append("%s \"%s\" %s NULL" % (alter_dic["command"], alter_dic["name"], column_type))	
			elif alter_dic["command"] == 'CHANGE':
				sql_rename = ""
				sql_type = ""
				old_column=alter_dic["old"]
				new_column=alter_dic["new"]
				column_type=self.type_dictionary[alter_dic["type"]]
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				sql_type = """ALTER TABLE "%s" ALTER COLUMN "%s" SET DATA TYPE %s  USING "%s"::%s ;;""" % (table_name, old_column, column_type, old_column, column_type)
				if old_column != new_column:
					sql_rename="""ALTER TABLE  "%s" RENAME COLUMN "%s" TO "%s" ;""" % (table_name, old_column, new_column)
				query=sql_type+sql_rename
				return query
			elif alter_dic["command"] == 'MODIFY':
				column_type=self.type_dictionary[alter_dic["type"]]
				column_name=alter_dic["name"]
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				query = """ALTER TABLE "%s" ALTER COLUMN "%s" SET DATA TYPE %s USING "%s"::%s ;""" % (table_name, column_name, column_type, column_name, column_type)
				return query
		query=' '.join(ddl_enum)+" "+query_cmd + ' '+ table_name+ ' ' +', '.join(alter_cmd)+" ;"
		return query

	
					

	def drop_primary_key(self, token):
		self.logger.info("dropping primary key for table %s" % (token["name"],))
		sql_gen="""
						SELECT  DISTINCT
							format('ALTER TABLE %%I.%%I DROP CONSTRAINT %%I;',
							table_schema,
							table_name,
							constraint_name
							)
						FROM 
							information_schema.key_column_usage 
						WHERE 
								table_schema=%s 
							AND table_name=%s;
					"""
		self.pg_conn.pgsql_cur.execute(sql_gen, (self.pg_conn.dest_schema, token["name"]))
		value_check=self.pg_conn.pgsql_cur.fetchone()
		if value_check:
			sql_drop=value_check[0]
			self.pg_conn.pgsql_cur.execute(sql_drop)
			self.unregister_table(token["name"])

	def gen_query(self, token):
		""" the function generates the ddl"""
		query=""
		
		if token["command"] =="DROP TABLE":
			query=" %(command)s IF EXISTS \"%(name)s\" CASCADE;" % token
		elif token["command"] =="TRUNCATE":
			query=" %(command)s TABLE \"%(name)s\" CASCADE;" % token
		elif token["command"] =="CREATE TABLE":
			table_metadata={}
			table_metadata["columns"]=token["columns"]
			table_metadata["name"]=token["name"]
			table_metadata["indices"]=token["indices"]
			self.table_metadata={}
			self.table_metadata[token["name"]]=table_metadata
			self.build_tab_ddl()
			self.build_idx_ddl()
			query_type=' '.join(self.type_ddl[token["name"]])
			query_table=self.table_ddl[token["name"]]
			query_idx=' '.join(self.idx_ddl[token["name"]])
			query=query_type+query_table+query_idx
			self.store_table(token["name"])
		elif token["command"] == "ALTER TABLE":
			query=self.build_alter_table(token)
		elif token["command"] == "DROP PRIMARY KEY":
			self.drop_primary_key(token)
		return query 
		
		
	def write_ddl(self, token, query_data):
		sql_path=" SET search_path="+self.pg_conn.dest_schema+";"
		pg_ddl=sql_path+self.gen_query(token)
		log_table=query_data["log_table"]
		insert_vals=(	query_data["batch_id"], 
								token["name"],  
								query_data["schema"], 
								query_data["binlog"], 
								query_data["logpos"], 
								pg_ddl
							)
		sql_insert="""
								INSERT INTO sch_chameleon."""+log_table+"""
								(
									i_id_batch, 
									v_table_name, 
									v_schema_name, 
									enm_binlog_event, 
									t_binlog_name, 
									i_binlog_position, 
									t_query
								)
								VALUES
								(
									%s,
									%s,
									%s,
									'ddl',
									%s,
									%s,
									%s
								)
						"""
		self.pg_conn.pgsql_cur.execute(sql_insert, insert_vals)
		
	def truncate_table(self, table_name, schema_name):
		sql_clean="""
							SELECT 
								format('SET lock_timeout=''10s'';TRUNCATE TABLE %%I.%%I;',schemaname,tablename) v_truncate,
								format('DELETE FROM %%I.%%I;',schemaname,tablename) v_delete,
								format('VACUUM %%I.%%I;',schemaname,tablename) v_vacuum,
								format('%%I.%%I',schemaname,tablename) as v_tab,
								tablename    
							FROM 
								pg_tables
							WHERE
								tablename=%s
								AND schemaname=%s
						"""
		self.pg_conn.pgsql_cur.execute(sql_clean, (table_name, schema_name))
		tab_clean=self.pg_conn.pgsql_cur.fetchone()
		if  tab_clean:
			st_truncate=tab_clean[0]
			st_delete=tab_clean[1]
			st_vacuum=tab_clean[2]
			tab_name=tab_clean[3]
			try:
				self.logger.info("running truncate table on %s" % (tab_name,))
				self.pg_conn.pgsql_cur.execute(st_truncate)
				
			except:
				self.logger.info("truncate failed, fallback to delete on table %s" % (tab_name,))
				self.pg_conn.pgsql_cur.execute(st_delete)
				self.logger.info("running vacuum on table %s" % (tab_name,))
				self.pg_conn.pgsql_cur.execute(st_vacuum)
			return True
		else:
			return False
	
	def truncate_tables(self):
		table_limit = ''
		if self.table_limit[0] != '*':
			table_limit = self.pg_conn.pgsql_cur.mogrify("""WHERE v_table IN  (SELECT unnest(%s))""",(self.table_limit, )).decode()
		
		
		sql_clean=""" 
						SELECT DISTINCT
							format('SET lock_timeout=''10s'';TRUNCATE TABLE %%I.%%I;',v_schema,v_table) v_truncate,
							format('DELETE FROM %%I.%%I;',v_schema,v_table) v_delete,
							format('VACUUM %%I.%%I;',v_schema,v_table) v_vacuum,
							format('%%I.%%I',v_schema,v_table) as v_tab,
							v_table
						FROM
							sch_chameleon.t_index_def 
						%s
						
						ORDER BY 
							v_table
		""" % (table_limit, )
		self.pg_conn.pgsql_cur.execute(sql_clean)
		tab_clean=self.pg_conn.pgsql_cur.fetchall()
		for stat_clean in tab_clean:
			st_truncate=stat_clean[0]
			st_delete=stat_clean[1]
			st_vacuum=stat_clean[2]
			tab_name=stat_clean[3]
			try:
				self.logger.info("truncating table %s" % (tab_name,))
				self.pg_conn.pgsql_cur.execute(st_truncate)
				
			except:
				self.logger.info("truncate failed, fallback to delete on table %s" % (tab_name,))
				self.pg_conn.pgsql_cur.execute(st_delete)
				self.logger.info("running vacuum on table %s" % (tab_name,))
				self.pg_conn.pgsql_cur.execute(st_vacuum)

	def get_index_def(self):
		table_limit = ''
		if self.table_limit[0] != '*':
			table_limit = self.pg_conn.pgsql_cur.mogrify("""WHERE table_name IN  (SELECT unnest(%s))""",(self.table_limit, )).decode()
		
		drp_msg = 'Do you want to clean the existing index definitions in t_index_def?.\n YES/No\n' 
		if sys.version_info[0] == 3:
			drop_idx = input(drp_msg)
		else:
			drop_idx = raw_input(drp_msg)
		if drop_idx == 'YES':
			sql_delete = """ DELETE FROM sch_chameleon.t_index_def;"""
			self.pg_conn.pgsql_cur.execute(sql_delete)
		elif drop_idx in self.lst_yes or len(drop_idx) == 0:
			print('Please type YES all uppercase to confirm')
			sys.exit()
		self.logger.info("collecting indices and pk for schema %s" % (self.pg_conn.dest_schema,))
		
		sql_get_idx=""" 
				
				INSERT INTO sch_chameleon.t_index_def
					(
						v_schema,
						v_table,
						v_index,
						t_create,
						t_drop
					)
				SELECT 
					schema_name,
					table_name,
					index_name,
					CASE
						WHEN indisprimary
						THEN
							format('ALTER TABLE %%I.%%I ADD CONSTRAINT %%I %%s',
								schema_name,
								table_name,
								index_name,
								pg_get_constraintdef(const_id)
							)
							
						ELSE
							pg_get_indexdef(index_id)    
					END AS t_create,
					CASE
						WHEN indisprimary
						THEN
							format('ALTER TABLE %%I.%%I DROP CONSTRAINT %%I CASCADE',
								schema_name,
								table_name,
								index_name
								
							)
							
						ELSE
							format('DROP INDEX %%I.%%I',
								schema_name,
								index_name
								
							)
					END AS  t_drop
					
				FROM

				(
				SELECT 
					tab.relname AS table_name,
					indx.relname AS index_name,
					idx.indexrelid index_id,
					indisprimary,
					sch.nspname schema_name,
					cns.oid as const_id
					
				FROM
					pg_index idx
					INNER JOIN pg_class indx
					ON
						idx.indexrelid=indx.oid
					INNER JOIN pg_class tab
					INNER JOIN pg_namespace sch
					ON 
						tab.relnamespace=sch.oid
					
					ON
						idx.indrelid=tab.oid
					LEFT OUTER JOIN pg_constraint cns
					ON 
							indx.relname=cns.conname
						AND cns.connamespace=sch.oid
					
				WHERE
					sch.nspname=%s
				) idx
		""" + table_limit +""" ON CONFLICT DO NOTHING"""
		self.pg_conn.pgsql_cur.execute(sql_get_idx, (self.pg_conn.dest_schema, ))
		
	
	def drop_src_indices(self):
		table_limit = ''
		if self.table_limit[0] != '*':
			table_limit = self.pg_conn.pgsql_cur.mogrify("""WHERE v_table IN  (SELECT unnest(%s))""",(self.table_limit, )).decode()
		
		sql_idx="""SELECT t_drop FROM  sch_chameleon.t_index_def %s; """ % (table_limit, )
		self.pg_conn.pgsql_cur.execute(sql_idx)
		idx_drop=self.pg_conn.pgsql_cur.fetchall()
		for drop_stat in idx_drop:
			self.pg_conn.pgsql_cur.execute(drop_stat[0])
			
	def create_src_indices(self):
		table_limit = ''
		if self.table_limit[0] != '*':
			table_limit = self.pg_conn.pgsql_cur.mogrify("""WHERE v_table IN  (SELECT unnest(%s))""",(self.table_limit, )).decode()
		
		sql_idx="""SELECT t_create FROM  sch_chameleon.t_index_def %s;""" % (table_limit, )
		self.pg_conn.pgsql_cur.execute(sql_idx)
		idx_create=self.pg_conn.pgsql_cur.fetchall()
		for create_stat in idx_create:
			self.pg_conn.pgsql_cur.execute(create_stat[0])
	
	def add_source(self, source_name, schema_clear, schema_obf):
		sql_source = """
					SELECT 
						count(i_id_source)
					FROM 
						sch_chameleon.t_sources 
					WHERE 
						t_source=%s
				;
			"""
		self.pg_conn.pgsql_cur.execute(sql_source, (source_name, ))
		source_data = self.pg_conn.pgsql_cur.fetchone()
		cnt_source = source_data[0]
		if cnt_source == 0:
			sql_add = """INSERT INTO sch_chameleon.t_sources 
						( t_source,t_dest_schema,t_obf_schema) 
					VALUES 
						(%s,%s,%s); """
			self.pg_conn.pgsql_cur.execute(sql_add, (source_name, schema_clear, schema_obf ))
		else:
			print("Source %s already registered." % source_name)
		sys.exit()
	def drop_source(self, source_name):
		sql_delete = """ DELETE FROM sch_chameleon.t_sources 
					WHERE  t_source=%s; """
		self.pg_conn.pgsql_cur.execute(sql_delete, (source_name, ))
	
	def get_source_status(self, source_name):
		sql_source = """
					SELECT 
						enm_status
					FROM 
						sch_chameleon.t_sources 
					WHERE 
						t_source=%s
				;
			"""
		self.pg_conn.pgsql_cur.execute(sql_source, (source_name, ))
		source_data = self.pg_conn.pgsql_cur.fetchone()
		if source_data:
			source_status = source_data[0]
		else:
			source_status = 'Not registered'
		return source_status
		
	def get_status(self):
		"""the function list the sources with the running status and the eventual lag """
		sql_status="""
								SELECT
									t_source,
									t_dest_schema,
									enm_status,
									extract(epoch from now()-ts_last_event)::integer as i_seconds_behind_master,
									ts_last_event ,
									t_obf_schema
								FROM 
									sch_chameleon.t_sources
								ORDER BY 
									t_source
								; """
		self.pg_conn.pgsql_cur.execute(sql_status)
		results = self.pg_conn.pgsql_cur.fetchall()
		return results
		
	def set_source_id(self, source_status):
		sql_source = """
					UPDATE sch_chameleon.t_sources
					SET
						enm_status=%s
					WHERE
						t_source=%s
					RETURNING i_id_source,t_dest_schema,t_obf_schema
				;
			"""
		source_name=self.pg_conn.global_conf.source_name
		self.pg_conn.pgsql_cur.execute(sql_source, (source_status, source_name))
		source_data=self.pg_conn.pgsql_cur.fetchone()
		try:
			self.i_id_source=source_data[0]
			self.dest_schema=source_data[1]
			self.obf_schema=source_data[2]
		except:
			print("Source %s is not registered." % source_name)
			sys.exit()
	
			
	def clean_batch_data(self):
		sql_delete="""DELETE FROM sch_chameleon.t_replica_batch 
								WHERE i_id_source=%s;
							"""
		self.pg_conn.pgsql_cur.execute(sql_delete, (self.i_id_source, ))
	
