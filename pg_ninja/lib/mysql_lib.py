import StringIO
import pymysql
import sys
import codecs
import binascii
import hashlib
import datetime
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import RotateEvent
from pg_ninja import sql_token

class mysql_connection:
	"""
		Class to manage the connection with the mysql database.
		It uses the data imported by the class global_conf. 
		The database name is in separate attribute because the mysql replication 
		requires a dictionary without the database. 
		However the conventional connection	requires the database to connect to.
		The connect_db method builds the connection string in the correct format.
		
		:param global_config: The :class:`global_lib.global_config` object instantiated in :class:`global_lib.replica_engine` 
	"""
	def __init__(self, global_config):
		self.global_conf=global_config
		self.my_server_id=self.global_conf.my_server_id
		self.mysql_conn=self.global_conf.mysql_conn
		self.my_database=self.global_conf.my_database
		self.my_charset=self.global_conf.my_charset
		self.tables_limit=self.global_conf.tables_limit
		self.exclude_tables=self.global_conf.exclude_tables
		self.replica_batch_size=self.global_conf.replica_batch_size
		self.copy_mode=self.global_conf.copy_mode
		self.my_connection=None
		self.my_cursor=None
		self.my_cursor_fallback=None
	
	
	
	def connect_snapshot_ubf(self, snap_data):
		self.my_connection_ubf=pymysql.connect(host=snap_data["host"],
							user=snap_data["user"],
							password=snap_data["passwd"],
							db=snap_data["my_database"],
							charset=snap_data["my_charset"],
							cursorclass=pymysql.cursors.SSCursor)
							
		self.my_cursor_ubf=self.my_connection_ubf.cursor()
		
	
	def connect_snapshot(self, snap_data):
		self.my_connection_ubf=pymysql.connect(host=snap_data["host"],
							user=snap_data["user"],
							password=snap_data["passwd"],
							db=snap_data["my_database"],
							charset=snap_data["my_charset"],
							cursorclass=pymysql.cursors.SSCursor)
							
		self.my_connection=pymysql.connect(host=snap_data["host"],
							user=snap_data["user"],
							password=snap_data["passwd"],
							db=snap_data["my_database"],
							charset=snap_data["my_charset"],
							cursorclass=pymysql.cursors.DictCursor)
							
		self.my_cursor_ubf=self.my_connection_ubf.cursor()
		self.my_cursor=self.my_connection.cursor()
		self.my_cursor_fallback=self.my_connection.cursor()
	
		
		
	def connect_db_ubf(self):
		"""  Establish connection with the database """
		self.my_connection_ubf=pymysql.connect(host=self.mysql_conn["host"],
							user=self.mysql_conn["user"],
							password=self.mysql_conn["passwd"],
							db=self.my_database,
							charset=self.my_charset,
							cursorclass=pymysql.cursors.SSCursor)
		self.my_cursor_ubf=self.my_connection_ubf.cursor()

	def connect_db(self):
		"""  
			Establish connection with the database using the parameters set in the class constructor.
		"""
		self.my_connection=pymysql.connect(host=self.mysql_conn["host"],
									user=self.mysql_conn["user"],
									password=self.mysql_conn["passwd"],
									db=self.my_database,
									charset=self.my_charset,
									cursorclass=pymysql.cursors.DictCursor)
		self.my_cursor=self.my_connection.cursor()
		self.my_cursor_fallback=self.my_connection.cursor()
		
	def disconnect_db(self):
		self.my_connection.close()
	
	def disconnect_db_ubf(self):
		self.my_connection_ubf.close()
		
	def disconnect_snapshot(self):
		self.my_connection.close()
		self.my_cursor_fallback.close()
		
	
	def disconnect_snapshot_ubf(self):
		self.my_connection_ubf.close()
		
class mysql_engine:
	"""
		Class to manage the mysql connection, copy and replica.
		The class methods can extract the table's metadata, copy the from mysql to postgresql, obfuscate the data 
		and replicate (and obfuscate) the changes between mysql and postgresql.
		
		:param global_config: The global config object
		:param logger: the logger object used to log the messages with different levels
		:param out_dir: the output directory for the data copy when the copy method is file
	"""
	def __init__(self, global_config, logger, out_dir="/tmp/"):
		self.hexify=global_config.hexify
		self.obfdic=global_config.obfdic
		self.logger=logger
		self.out_dir=out_dir
		self.my_tables={}
		self.mysql_con=mysql_connection(global_config)
		try:
			self.mysql_con.connect_db()
			self.get_table_metadata()
		except:
			pass
		self.my_streamer=None
		self.replica_batch_size=self.mysql_con.replica_batch_size
		self.master_status=[]
		self.id_batch=None
		self.schema_clear=global_config.schema_clear
		self.schema_obf=global_config.schema_obf
		self.copy_override=global_config.copy_override
		self.sql_token=sql_token()
		
			
	def obfuscate_value(self, column_value, obf_mode, column_data_type):
		"""
			performs obfuscation on the fly for the column 
		"""
		if column_value:
			if obf_mode["mode"]=="normal" :
				max_length=column_data_type["character_maximum_length"]
				obf=hashlib.sha256()
				if obf_mode["nonhash_length"]==0:
					obf.update(column_value.encode('utf-8'))
					column_value=obf.hexdigest()
				if obf_mode["nonhash_length"]>0:
					prefix_start=obf_mode["nonhash_start"]-1
					prefix_end=prefix_start+obf_mode["nonhash_length"]
					col_prefix=column_value[prefix_start:prefix_end]
					obf.update(column_value.encode('utf-8'))
					column_value=col_prefix+str(obf.hexdigest())
				column_value=column_value[0:max_length]
			elif obf_mode["mode"]=="date":
				column_value=column_value.replace(day=01,month=01)
			elif obf_mode["mode"]=="numeric":
				column_value='0'
			elif obf_mode["mode"] == "setnull":
				column_value=None
		return column_value
	
	def write_query(self, tokenised, pg_engine, query_data):
		
		for token in tokenised:
			if token!={}:
				query_data["name"]=token["name"]
				if token["command"]=='CREATE TABLE':
					self.get_table_metadata(token["name"])
					pg_token=self.my_tables
				else:
					pg_token=token
				pg_engine.write_ddl(pg_token, query_data, token["command"], self.mysql_con.my_cursor, self.mysql_con.my_database)

	def read_replica(self, batch_data, pg_engine):
		"""
		Stream the replica using the batch data.
		
		:param batch_data: 
			List: [id_batch,log_file,log_position,log_table]
		:returns List: [master_data={File,Position} , group_insert={global_data,event_data} ]
	
		"""
		table_type_map=self.get_table_type_map()	
		sql_tokenise=sql_token()
		close_batch=False
		total_events=0
		master_data={}
		group_insert=[]
		total_events=0
		id_batch=batch_data[0][0]
		log_file=batch_data[0][1]
		log_position=batch_data[0][2]
		log_table=batch_data[0][3]
		my_stream = BinLogStreamReader(
																connection_settings = self.mysql_con.mysql_conn, 
																server_id =self.mysql_con.my_server_id, 
																only_events = [RotateEvent, QueryEvent,DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent], 
																log_file = log_file, 
																log_pos = log_position, 
																resume_stream = True, 
																only_schemas = [self.mysql_con.my_database]
														)
		self.logger.debug("START STREAMING - log_file %s, log_position %s. id_batch: %s " % (log_file, log_position, id_batch))
		for binlogevent in my_stream:
			total_events+=1
			if isinstance(binlogevent, RotateEvent):
				binlogfile=binlogevent.next_binlog
				position=binlogevent.position
				self.logger.debug("ROTATE EVENT - binlogfile %s, position %s. " % (binlogfile, position))
				if close_batch:
					if log_file!=binlogfile:
						master_data["File"]=binlogfile
						master_data["Position"]=position
					if len(group_insert)>0:
						pg_engine.write_batch(group_insert)
						group_insert=[]
					my_stream.close()
					return [master_data, close_batch]
				
			elif isinstance(binlogevent, QueryEvent):
				grp_length = len(group_insert)
				if len(group_insert)>0:
					self.logger.debug("WRITING GROUP - binlogfile %s, position %s. Lenght group insert: %s \n rows: %s " % (binlogfile, binlogevent.packet.log_pos, grp_length, len(group_insert) ))
					pg_engine.write_batch(group_insert)
					group_insert=[]
				self.sql_token.parse_sql(binlogevent.query)
				print self.sql_token.tokenised
				for token in self.sql_token.tokenised:
					if len(token)>0:
						master_data["File"]=binlogfile
						master_data["Position"]=binlogevent.packet.log_pos
						self.logger.debug("CAPTURED QUERY- binlogfile %s, position %s. Lenght group insert: %s \n Query: %s " % (binlogfile, binlogevent.packet.log_pos, grp_length, binlogevent.query))
						query_data={
									"binlog":log_file, 
									"logpos":log_position, 
									"schema": self.schema_clear, 
									"batch_id":id_batch, 
									"log_table":log_table
						}
						pg_engine.write_ddl(token, query_data)
						close_batch=True
					
				self.sql_token.reset_lists()
				if close_batch:
					my_stream.close()
					return [master_data, close_batch]
				
			else:
				
				for row in binlogevent.rows:
					total_events+=1
					log_file=binlogfile
					log_position=binlogevent.packet.log_pos
					table_name=binlogevent.table
					column_map=table_type_map[table_name]
					
					
					global_data={
										"binlog":log_file, 
										"logpos":log_position, 
										"schema": self.schema_clear, 
										"table": table_name, 
										"batch_id":id_batch, 
										"log_table":log_table
									}
					event_data={}
					event_data_obf={}
					if isinstance(binlogevent, DeleteRowsEvent):
						global_data["action"] = "delete"
						event_values=row["values"]
					elif isinstance(binlogevent, UpdateRowsEvent):
						global_data["action"] = "update"
						event_values=row["after_values"]
					elif isinstance(binlogevent, WriteRowsEvent):
						global_data["action"] = "insert"
						event_values=row["values"]
					global_obf=dict(global_data.items())
					global_obf["schema"]=self.schema_obf
					for column_name in event_values:
						column_data_type=column_map[column_name]
						column_type=column_data_type["data_type"]
						if column_type in self.hexify and event_values[column_name]:
							event_values[column_name]=binascii.hexlify(event_values[column_name])
					
					
					try:
						obf_list=self.obfdic[table_name]
						event_values_obf=dict(event_values.items())
					except:
						obf_list=None
						event_values_obf=None
					
					if obf_list:
						try:
							for column_name in obf_list:
								obf_mode=obf_list[column_name]
								event_values_obf[column_name]=self.obfuscate_value(event_values_obf[column_name], obf_mode, column_data_type)
						except:
							self.logger.error("discarded row in obfuscation process.\n global_data:%s \n event_data:%s \n" % (global_data,event_values ))
							
					event_data = dict(event_data.items() +event_values.items())
					event_insert={"global_data":global_data,"event_data":event_data}
					group_insert.append(event_insert)
					
					if event_values_obf:
						event_data_obf = dict(event_data_obf.items() +event_values_obf.items())
						event_obf={"global_data":global_obf,"event_data":event_data_obf }
						group_insert.append(event_obf)
							
					
					master_data["File"]=log_file
					master_data["Position"]=log_position
					if total_events>=self.replica_batch_size:
						self.logger.debug("total events exceeded. Master data: %s  " % (master_data,  ))
						total_events=0
						pg_engine.write_batch(group_insert)
						group_insert=[]
						close_batch=True
						
		my_stream.close()
		if len(group_insert)>0:
			pg_engine.write_batch(group_insert)
			close_batch=True
		return [master_data, close_batch]

	def run_replica(self, pg_engine):
		"""
		Reads the MySQL replica and stores the data in postgres. 
		
		:param pg_engine: The postgresql engine object required for storing the master coordinates and replaying the batches
		"""
		batch_data=pg_engine.get_batch_data()
		self.logger.debug('batch data: %s' % (batch_data, ))
		if len(batch_data)>0:
			id_batch=batch_data[0][0]
			replica_data=self.read_replica(batch_data, pg_engine)
			master_data=replica_data[0]
			close_batch=replica_data[1]
			if close_batch:
				self.master_status=[]
				self.master_status.append(master_data)
				self.logger.debug("trying to save the master data...")
				next_id_batch=pg_engine.save_master_status(self.master_status)
				if next_id_batch:
					self.logger.debug("new batch created, saving id_batch %s in class variable" % (id_batch))
					self.id_batch=id_batch
				else:
					self.logger.debug("batch not saved. using old id_batch %s" % (self.id_batch))
				if self.id_batch:
					self.logger.debug("updating processed flag for id_batch %s", (id_batch))
					pg_engine.set_batch_processed(id_batch)
					self.id_batch=None
		self.logger.debug("replaying batch.")
		pg_engine.process_batch(self.replica_batch_size)
		

	
		
	def get_table_type_map(self):
		"""
			Builds a table/type map used in the hexification process for binary data and obfuscation process.
			The field character_maximum_length is used to truncate the sha256 hashes to the correct size when inserting
			in PostgreSQL.
			
		"""
		table_type_map={}
		self.logger.debug("collecting table type map")
		sql_tables="""SELECT 
											table_schema,
											table_name
								FROM 
											information_schema.TABLES 
								WHERE 
														table_type='BASE TABLE' 
											AND 	table_schema=%s
								;
							"""
		self.mysql_con.my_cursor.execute(sql_tables, (self.mysql_con.my_database))
		table_list=self.mysql_con.my_cursor.fetchall()
		for table in table_list:
			column_type={}
			column_type_data={}
			sql_columns="""SELECT 
												column_name,
												data_type,
												character_maximum_length
									FROM 
												information_schema.COLUMNS 
									WHERE 
															table_schema=%s
												AND 	table_name=%s
									ORDER BY 
													ordinal_position
									;
								"""
			self.mysql_con.my_cursor.execute(sql_columns, (self.mysql_con.my_database, table["table_name"]))
			column_data=self.mysql_con.my_cursor.fetchall()
			for column in column_data:
				column_type_data={}
				column_type_data["data_type"]=column["data_type"]
				column_type_data["character_maximum_length"]=column["character_maximum_length"]
				column_type[column["column_name"]]=column_type_data
			table_type_map[table["table_name"]]=column_type
		return table_type_map
		
			
		
	def get_column_metadata(self, table, obf_list):
		
		date_fields=[]
		normal_noprfx=[]
		normal_prfx=[]
		sql_prfx=["""SELECT ' ' as column_name,  -100 as nonhash_start, -100 as nonhash_length"""]
		
		if obf_list:
			for field in obf_list:
				if obf_list[field]["mode"]=="date":
					date_fields.append(field)
				elif obf_list[field]["mode"]=="normal" and obf_list[field]["nonhash_length"]==0:
					normal_noprfx.append(field)
				elif obf_list[field]["mode"]=="normal" and obf_list[field]["nonhash_length"]>0:
					normal_prfx.append(field)
					sql_prfx.append("""SELECT '%s' as column_name,  %s as nonhash_start, %s as nonhash_length""" % (field, obf_list[field]["nonhash_start"], obf_list[field]["nonhash_length"]))
		sql_substr=' UNION '.join(sql_prfx)
		
		
		
		sql_columns="""SELECT 
											column_name,
											column_default,
											ordinal_position,
											data_type,
											character_maximum_length,
											extra,
											column_key,
											is_nullable,
											numeric_precision,
											numeric_scale,
											CASE 
												WHEN data_type="enum"
											THEN	
												SUBSTRING(COLUMN_TYPE,5)
											END AS enum_list,
											CASE
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned)')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_csv_clear,
											CASE
												WHEN
													column_name IN ('"""+"','".join(date_fields)+"""')
												THEN
													concat('DATE_FORMAT(`',column_name,'`,','''%%Y-01-01'')')
												WHEN
													column_name IN ('"""+"','".join(normal_noprfx)+"""')
												THEN
													concat('substr(','sha2(`',column_name,'`,256),1,',character_maximum_length,')' )
												WHEN
													column_name IN ('"""+"','".join(normal_prfx)+"""')
												THEN
													(
													SELECT 
															concat(
																		'substr(',
																		'concat(substr(`',column_name,'`,',nonhash_start,',',nonhash_length,')',','
																		'sha2(`',column_name,'`,256)',
																		'),',
																		'1,',
																		character_maximum_length,
																		')'
																		
																	)
														FROM
														( """ + sql_substr + """) prefix
														WHERE
															prefix.column_name=information_schema.COLUMNS.column_name
														
													)
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned)')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_csv_obf,
											CASE
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned) AS','`',column_name,'`')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_select_clear,
											CASE
												WHEN
													column_name IN ('"""+"','".join(date_fields)+"""')
												THEN
													concat('DATE_FORMAT(`',column_name,'`,','''%%Y-01-01'') AS','`',column_name,'`')
												WHEN
													column_name IN ('"""+"','".join(normal_noprfx)+"""')
												THEN
													concat('substr(','sha2(`',column_name,'`,256),1,',character_maximum_length,') AS','`',column_name,'`')
												WHEN
													column_name IN ('"""+"','".join(normal_prfx)+"""')
												THEN
													(
													SELECT 
															concat(
																			'substr(',
																			'concat(substr(`',column_name,'`,',nonhash_start,',',nonhash_length,')',','
																			'sha2(`',column_name,'`,256)',
																			'),',
																			'1,',
																			character_maximum_length,
																			') as `',
																			column_name,
																			'`'
																			
																		)
														FROM
														( """ + sql_substr + """) prefix
														WHERE
															prefix.column_name=information_schema.COLUMNS.column_name
														
													)
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned) AS','`',column_name,'`')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_select_obf
								FROM 
											information_schema.COLUMNS 
								WHERE 
														table_schema=%s
											AND 	table_name=%s
								ORDER BY 
												ordinal_position
								;
							"""
		self.mysql_con.my_cursor.execute(sql_columns, (self.mysql_con.my_database, table))
		column_data=self.mysql_con.my_cursor.fetchall()
		return column_data

	def get_index_metadata(self, table):
		sql_index="""SELECT 
										index_name,
										non_unique,
										GROUP_CONCAT(concat('"',column_name,'"') ORDER BY seq_in_index) as index_columns
									FROM
										information_schema.statistics
									WHERE
														table_schema=%s
											AND 	table_name=%s
											AND	index_type = 'BTREE'
									GROUP BY 
										table_name,
										non_unique,
										index_name
									;
							"""
		self.mysql_con.my_cursor.execute(sql_index, (self.mysql_con.my_database, table))
		index_data=self.mysql_con.my_cursor.fetchall()
		return index_data
	
	def get_table_metadata(self, table_check=None):
		self.my_tables={}
		self.logger.debug("getting table metadata")
		table_include=""
		table_exclude=""
		if table_check:
			self.logger.debug("extracting the table's metadata for: %s" % (table_check, ))
			table_include="AND table_name='"+table_check+"'"
		else:
			if self.mysql_con.tables_limit:
				self.logger.debug("table copy limited to tables: %s" % ','.join(self.mysql_con.tables_limit))
				table_include="AND table_name IN ('"+"','".join(self.mysql_con.tables_limit)+"')"
			if self.mysql_con.exclude_tables:
				self.logger.debug("excluding from the copy and replica the tables: %s" % ','.join(self.mysql_con.exclude_tables))
				table_exclude="AND table_name NOT IN ('"+"','".join(self.mysql_con.exclude_tables)+"')"
		sql_tables="""SELECT 
											table_schema,
											table_name
								FROM 
											information_schema.TABLES 
								WHERE 
														table_type='BASE TABLE' 
											AND 	table_schema=%s
											""" + table_include + """
											""" + table_exclude + """
								ORDER BY AVG_ROW_LENGTH DESC
								;
							"""
		
		self.mysql_con.my_cursor.execute(sql_tables, (self.mysql_con.my_database))
		table_list=self.mysql_con.my_cursor.fetchall()
		for table in table_list:
			try:
				obf_list=self.obfdic[table["table_name"]]
			except:
				obf_list=None
			column_data=self.get_column_metadata(table["table_name"], obf_list)
			index_data=self.get_index_metadata(table["table_name"])
			dic_table={'name':table["table_name"], 'columns':column_data,  'indices': index_data}
			self.my_tables[table["table_name"]]=dic_table
			
	def print_progress (self, iteration, total, table_name):
		if total>1:
			self.logger.info("Table %s copied %s %%" % (table_name, round(100 * float(iteration)/float(total), 1)))
		else:
			self.logger.info("Table %s copied %s %%" % (table_name, round(100 * float(iteration)/float(total), 1)))
		
	def generate_select(self, table_columns, mode="csv"):
		column_list=[]
		columns=""
		if mode=="csv":
			for column in table_columns:
					column_list.append("COALESCE(REPLACE("+column["column_csv_clear"]+", '\"', '\"\"'),'NULL') ")
			columns="REPLACE(CONCAT('\"',CONCAT_WS('\",\"',"+','.join(column_list)+"),'\"'),'\"NULL\"','NULL')"
		if mode=="insert":
			for column in table_columns:
				column_list.append(column["column_select_clear"])
			columns=','.join(column_list)
		return columns
	
	def insert_table_data(self, pg_engine, ins_arg):
		"""fallback to inserts for table and slices """
		slice_insert=ins_arg[0]
		table_name=ins_arg[1]
		columns_ins=ins_arg[2]
		copy_limit=ins_arg[3]
		total_slices=len(slice_insert)
		current_slice=1
		self.logger.info("Executing inserts for remaining %s slices for table %s. copy limit %s" % (total_slices, table_name, copy_limit))
		for slice in slice_insert:
			self.logger.info("Processing slice %s of %s" % (current_slice, total_slices))
			sql_out="SELECT "+columns_ins+"  FROM "+table_name+" LIMIT "+str(slice*copy_limit)+", "+str(copy_limit)+";"
			self.mysql_con.my_cursor_fallback.execute(sql_out)
			insert_data =  self.mysql_con.my_cursor_fallback.fetchall()
			pg_engine.insert_data(table_name, insert_data , self.my_tables)
			current_slice=current_slice+1
	def copy_table_data(self, pg_engine,  limit=10000,  copy_obfuscated=True):
		out_file='/tmp/output_copy.csv'
		self.logger.info("locking the tables")
		self.lock_tables()
		for table_name in self.my_tables:
			slice_insert=[]
			try:
				copy_limit=self.copy_override[table_name]
				self.logger.info("Overriding copy max limit for table %s to %s" % (table_name, copy_limit))
			except:
				copy_limit=limit
				self.logger.debug("Copy max size is %s for table %s " % (copy_limit, table_name))
			
			self.logger.info("copying table "+table_name)
			table=self.my_tables[table_name]
			
			table_name=table["name"]
			table_columns=table["columns"]
			self.logger.debug("estimating rows in "+table_name)
			sql_count="""
								SELECT 
										table_rows
									FROM 
										information_schema.TABLES 
									WHERE 
											table_schema=%s 
										AND	table_type='BASE TABLE'
										AND table_name=%s 
									;
	
							"""
			self.mysql_con.my_cursor.execute(sql_count, (self.mysql_con.my_database, table_name))
			count_rows=self.mysql_con.my_cursor.fetchone()
			num_slices=count_rows["table_rows"]/copy_limit
			range_slices=range(num_slices+1)
			total_slices=len(range_slices)
			self.logger.debug(table_name +" will be copied in "+str(total_slices)+" slices" )
			columns_csv=self.generate_select(table_columns, mode="csv")
			columns_ins=self.generate_select(table_columns, mode="insert")
			slice=range_slices[0]
			
			csv_data=""
			sql_out="SELECT "+columns_csv+" as data FROM "+table_name+";"
			self.mysql_con.connect_db_ubf()
			try:
				self.logger.debug("Executing query for table %s"  % (table_name, ))
				self.mysql_con.my_cursor_ubf.execute(sql_out)
			except:
				self.logger.debug("an error occurred when pulling out the data from the table %s - sql executed: %s" % (table_name, sql_out))
					
			while True:
				csv_results = self.mysql_con.my_cursor_ubf.fetchmany(copy_limit)
				if len(csv_results) == 0:
					break
				csv_data="\n".join(d[0] for d in csv_results )
				
				if self.mysql_con.copy_mode=='direct':
					csv_file=StringIO.StringIO()
					csv_file.write(csv_data)
					csv_file.seek(0)

				if self.mysql_con.copy_mode=='file':
					csv_file=codecs.open(out_file, 'wb', self.mysql_con.my_charset)
					csv_file.write(csv_data)
					csv_file.close()
					csv_file=open(out_file, 'rb')
					
				try:
					pg_engine.copy_data(table_name, csv_file, self.my_tables)
				except:
					self.logger.info("table %s error in PostgreSQL copy, saving slice number for the fallback to insert statements " % (table_name, ))
					slice_insert.append(slice)
				self.print_progress(slice+1,total_slices, table_name)
				slice+=1
				csv_file.close()
			self.mysql_con.disconnect_db_ubf()
			if len(slice_insert)>0:
				ins_arg=[]
				ins_arg.append(slice_insert)
				ins_arg.append(table_name)
				ins_arg.append(columns_ins)
				ins_arg.append(copy_limit)
				self.insert_table_data(pg_engine, ins_arg)
		self.logger.info("releasing the lock")
		self.unlock_tables()
		if copy_obfuscated:
			pg_engine.copy_obfuscated(self.obfdic, self.mysql_con.tables_limit)
		
	def get_master_status(self):
		t_sql_master="SHOW MASTER STATUS;"
		self.mysql_con.my_cursor.execute(t_sql_master)
		self.master_status=self.mysql_con.my_cursor.fetchall()		
		
	def lock_tables(self):
		""" lock tables and get the log coords """
		self.locked_tables=[]
		for table_name in self.my_tables:
			table=self.my_tables[table_name]
			self.locked_tables.append(table["name"])
		t_sql_lock="FLUSH TABLES "+", ".join(self.locked_tables)+" WITH READ LOCK;"
		self.mysql_con.my_cursor.execute(t_sql_lock)
		self.get_master_status()
	
	def unlock_tables(self):
		""" unlock tables previously locked """
		t_sql_unlock="UNLOCK TABLES;"
		self.mysql_con.my_cursor.execute(t_sql_unlock)
		
	def __del__(self):
		try:
			self.mysql_con.disconnect_db()
		except:
			pass

class mysql_snapshot:
	def __init__(self, mysql_con, global_config, logger):
		out_dir="/tmp/"
		self.hexify=global_config.hexify
		self.logger=logger
		self.out_dir=out_dir
		self.my_tables={}
		self.my_streamer=None
		self.mysql_con=mysql_con
		#self.replica_batch_size=self.mysql_con.replica_batch_size
		#self.schema_clear=global_config.schema_clear
		#self.schema_obf=global_config.schema_obf
		#self.copy_override=global_config.copy_override
	
	def print_progress (self, iteration, total, table_name):
		if total>1:
			self.logger.info("Table %s copied %s %%" % (table_name, round(100 * float(iteration)/float(total), 1)))
		else:
			self.logger.info("Table %s copied %s %%" % (table_name, round(100 * float(iteration)/float(total), 1)))

	
	def get_column_metadata(self, table, obf_list={}):
		
		date_fields=[]
		normal_noprfx=[]
		normal_prfx=[]
		sql_prfx=["""SELECT ' ' as column_name,  -100 as nonhash_start, -100 as nonhash_length"""]
		
		if obf_list:
			for field in obf_list:
				if obf_list[field]["mode"]=="date":
					date_fields.append(field)
				elif obf_list[field]["mode"]=="normal" and obf_list[field]["nonhash_length"]==0:
					normal_noprfx.append(field)
				elif obf_list[field]["mode"]=="normal" and obf_list[field]["nonhash_length"]>0:
					normal_prfx.append(field)
					sql_prfx.append("""SELECT '%s' as column_name,  %s as nonhash_start, %s as nonhash_length""" % (field, obf_list[field]["nonhash_start"], obf_list[field]["nonhash_length"]))
		sql_substr=' UNION '.join(sql_prfx)
		
		
		
		sql_columns="""SELECT 
											column_name,
											column_default,
											ordinal_position,
											data_type,
											character_maximum_length,
											extra,
											column_key,
											is_nullable,
											numeric_precision,
											numeric_scale,
											CASE 
												WHEN data_type="enum"
											THEN	
												SUBSTRING(COLUMN_TYPE,5)
											END AS enum_list,
											CASE
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned)')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_csv_clear,
											CASE
												WHEN
													column_name IN ('"""+"','".join(date_fields)+"""')
												THEN
													concat('DATE_FORMAT(`',column_name,'`,','''%%Y-01-01'')')
												WHEN
													column_name IN ('"""+"','".join(normal_noprfx)+"""')
												THEN
													concat('substr(','sha2(`',column_name,'`,256),1,',character_maximum_length,')' )
												WHEN
													column_name IN ('"""+"','".join(normal_prfx)+"""')
												THEN
													(
													SELECT 
															concat(
																		'substr(',
																		'concat(substr(`',column_name,'`,',nonhash_start,',',nonhash_length,')',','
																		'sha2(`',column_name,'`,256)',
																		'),',
																		'1,',
																		character_maximum_length,
																		')'
																		
																	)
														FROM
														( """ + sql_substr + """) prefix
														WHERE
															prefix.column_name=information_schema.COLUMNS.column_name
														
													)
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned)')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_csv_obf,
											CASE
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned) AS','`',column_name,'`')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_select_clear,
											CASE
												WHEN
													column_name IN ('"""+"','".join(date_fields)+"""')
												THEN
													concat('DATE_FORMAT(`',column_name,'`,','''%%Y-01-01'') AS','`',column_name,'`')
												WHEN
													column_name IN ('"""+"','".join(normal_noprfx)+"""')
												THEN
													concat('substr(','sha2(`',column_name,'`,256),1,',character_maximum_length,') AS','`',column_name,'`')
												WHEN
													column_name IN ('"""+"','".join(normal_prfx)+"""')
												THEN
													(
													SELECT 
															concat(
																			'substr(',
																			'concat(substr(`',column_name,'`,',nonhash_start,',',nonhash_length,')',','
																			'sha2(`',column_name,'`,256)',
																			'),',
																			'1,',
																			character_maximum_length,
																			') as `',
																			column_name,
																			'`'
																			
																		)
														FROM
														( """ + sql_substr + """) prefix
														WHERE
															prefix.column_name=information_schema.COLUMNS.column_name
														
													)
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned) AS','`',column_name,'`')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_select_obf
								FROM 
											information_schema.COLUMNS 
								WHERE 
														table_schema=%s
											AND 	table_name=%s
								ORDER BY 
												ordinal_position
								;
							"""
		self.mysql_con.my_cursor.execute(sql_columns, (self.my_database, table))
		column_data=self.mysql_con.my_cursor.fetchall()
		return column_data
	
	def insert_table_data(self, pg_engine, ins_arg):
		"""fallback to inserts for table and slices """
		slice_insert=ins_arg[0]
		table_name=ins_arg[1]
		columns_ins=ins_arg[2]
		copy_limit=ins_arg[3]
		self.logger.info("Executing inserts for remaining %s slices for table %s. copy limit %s" % (len(slice_insert), table_name, copy_limit))
		for slice in slice_insert:
			sql_out="SELECT "+columns_ins+"  FROM "+table_name+" LIMIT "+str(slice*copy_limit)+", "+str(copy_limit)+";"
			self.mysql_con.my_cursor_fallback.execute(sql_out)
			insert_data =  self.mysql_con.my_cursor_fallback.fetchall()
			pg_engine.insert_data(table_name, insert_data , self.my_tables)
	def get_index_metadata(self, table):
		sql_index="""SELECT 
										index_name,
										non_unique,
										GROUP_CONCAT(concat('"',column_name,'"') ORDER BY seq_in_index) as index_columns
									FROM
										information_schema.statistics
									WHERE
														table_schema=%s
											AND 	table_name=%s
											AND	index_type = 'BTREE'
									GROUP BY 
										table_name,
										non_unique,
										index_name
									;
							"""
		self.mysql_con.my_cursor.execute(sql_index, (self.my_database, table))
		index_data=self.mysql_con.my_cursor.fetchall()
		return index_data
		
	def get_snapshot_metadata(self, snap_data):
		tables_limit=snap_data["tables_limit"]
		self.my_database=snap_data["my_database"]
		self.my_tables={}
		self.logger.debug("getting snapshot metadata")
		table_include=""
		if tables_limit:
			self.logger.debug("table copy limited to tables: %s" % ','.join(tables_limit))
			table_include="AND table_name IN ('"+"','".join(tables_limit)+"')"
		sql_tables="""SELECT 
											table_schema,
											table_name
								FROM 
											information_schema.TABLES 
								WHERE 
														table_type='BASE TABLE' 
											AND 	table_schema=%s
											""" + table_include + """
								ORDER BY AVG_ROW_LENGTH DESC
								;
							"""
		self.mysql_con.my_cursor.execute(sql_tables, (self.my_database, ))
		table_list=self.mysql_con.my_cursor.fetchall()
		for table in table_list:
			column_data=self.get_column_metadata(table["table_name"], )
			index_data=self.get_index_metadata(table["table_name"])
			dic_table={'name':table["table_name"], 'columns':column_data,  'indices': index_data}
			self.my_tables[table["table_name"]]=dic_table
		
	def generate_select(self, table_columns, mode="csv"):
		column_list=[]
		columns=""
		if mode=="csv":
			for column in table_columns:
					column_list.append("COALESCE(REPLACE("+column["column_csv_clear"]+", '\"', '\"\"'),'NULL') ")
			columns="REPLACE(CONCAT('\"',CONCAT_WS('\",\"',"+','.join(column_list)+"),'\"'),'\"NULL\"','NULL')"
		if mode=="insert":
			for column in table_columns:
				column_list.append(column["column_select_clear"])
			columns=','.join(column_list)
		return columns
	
	def copy_table_data(self, pg_engine, snap_data,  limit=10000):
		out_file='/tmp/output_copy.csv'
		for table_name in self.my_tables:
			slice_insert=[]
			copy_limit=limit
			self.logger.debug("Copy max size is %s for table %s " % (copy_limit, table_name))
			
			self.logger.info("copying table "+table_name)
			table=self.my_tables[table_name]
			
			table_name=table["name"]
			table_columns=table["columns"]
			self.logger.debug("counting rows in "+table_name)
			sql_count="""
								SELECT 
										table_rows
									FROM 
										information_schema.TABLES 
									WHERE 
											table_schema=%s 
										AND	table_type='BASE TABLE'
										AND table_name=%s 
									;
	
							"""
			self.mysql_con.my_cursor.execute(sql_count, (self.my_database, table_name))
			count_rows=self.mysql_con.my_cursor.fetchone()
			num_slices=count_rows["table_rows"]/copy_limit
			range_slices=range(num_slices+1)
			total_slices=len(range_slices)
			self.logger.debug(table_name +" will be copied in "+str(total_slices)+" slices" )
			columns_csv=self.generate_select(table_columns, mode="csv")
			columns_ins=self.generate_select(table_columns, mode="insert")
			slice=range_slices[0]
			
			csv_data=""
			sql_out="SELECT "+columns_csv+" as data FROM "+table_name+";"
			self.mysql_con.connect_snapshot_ubf(snap_data)
			try:
				self.logger.debug("Executing query for table %s"  % (table_name, ))
				self.mysql_con.my_cursor_ubf.execute(sql_out)
			except:
				self.logger.debug("an error occurred when pulling out the data from the table %s - sql executed: %s" % (table_name, sql_out))
				print self.mysql_con
					
			while True:
				csv_results = self.mysql_con.my_cursor_ubf.fetchmany(copy_limit)
				if len(csv_results) == 0:
					break
				csv_data="\n".join(d[0] for d in csv_results )
				
				if self.mysql_con.copy_mode=='direct':
					csv_file=StringIO.StringIO()
					csv_file.write(csv_data)
					csv_file.seek(0)

				if self.mysql_con.copy_mode=='file':
					csv_file=codecs.open(out_file, 'wb', self.mysql_con.my_charset)
					csv_file.write(csv_data)
					csv_file.close()
					csv_file=open(out_file, 'rb')
					
				try:
					pg_engine.copy_data(table_name, csv_file, self.my_tables)
				except:
					self.logger.info("table %s error in PostgreSQL copy, saving slice number for the fallback to insert statements " % (table_name, ))
					slice_insert.append(slice)
				self.print_progress(slice+1,total_slices, table_name)
				slice+=1
				csv_file.close()
			self.mysql_con.disconnect_snapshot_ubf()
			if len(slice_insert)>0:
				ins_arg=[]
				ins_arg.append(slice_insert)
				ins_arg.append(table_name)
				ins_arg.append(columns_ins)
				ins_arg.append(copy_limit)
				self.insert_table_data(pg_engine, ins_arg)
		
