from pg_ninja import mysql_connection, mysql_engine, pg_engine, mysql_snapshot, email_alerts
import yaml
import sys
import os
import time
import logging
from tabulate import tabulate
from logging.handlers  import TimedRotatingFileHandler
from datetime import datetime


class global_config(object):
	"""
		This class parses the configuration file which is in config/config.yaml and sets 
		the class variables used by the other libraries. 
		The constructor checks if the configuration file is present and if is missing emits an error message followed by
		the sys.exit() command. If the configuration file is successful parsed the class variables are set from the
		configuration values.
		The class sets the log output file from the parameter command.  If the log destination is stdout then the logfile is ignored
		
		:param command: the command specified on the ninja.py command line
	
	"""
	def __init__(self,  config_name="default"):
		"""
			Class  constructor.
		"""
		
		config_dir='config'
		config_file = '%s/%s.yaml' % (config_dir, config_name)
		obfuscation_file='config/obfuscation.yaml'
		self.snapshots_file='config/snapshots.yaml'
		if os.path.isfile(config_file):
			self.config_name = config_name
			self.config_dir = config_dir
		else:
			print("**FATAL - could not find the configuration file %s.yaml in %s"  % (config_name, config_dir))
			sys.exit(2)
		
		conffile=open(config_file, 'rb')
		confdic=yaml.load(conffile.read())
		conffile.close()
		
		
		
		try:
			self.mysql_conn=confdic["mysql_conn"]
			self.pg_conn=confdic["pg_conn"]
			self.my_database=confdic["my_database"]
			self.my_charset=confdic["my_charset"]
			self.pg_charset=confdic["pg_charset"]
			self.pg_database=confdic["pg_database"]
			self.my_server_id=confdic["my_server_id"]
			self.schema_clear=confdic["schema_clear"]
			self.schema_obf=confdic["schema_obf"]
			self.replica_batch_size=confdic["replica_batch_size"]
			self.reply_batch_size=confdic["reply_batch_size"]
			self.tables_limit=confdic["tables_limit"]
			self.exclude_tables=confdic["exclude_tables"]
			self.copy_max_size=confdic["copy_max_size"]
			self.copy_mode=confdic["copy_mode"]
			self.hexify=confdic["hexify"]
			self.log_level=confdic["log_level"]
			self.log_dest=confdic["log_dest"]
			self.copy_override=confdic["copy_override"]
			self.log_file=confdic["log_dir"]+"/"+config_name+'.log'
			self.pid_file=confdic["pid_dir"]+"/replica.pid"
			self.log_dir=confdic["log_dir"]
			self.email_config=confdic["email_config"]
			self.log_days_keep = confdic["log_days_keep"]
			self.skip_view = confdic["skip_view"]
			self.out_dir = confdic["out_dir"]
			self.source_name= confdic["source_name"]
			if confdic["obfuscation_file"]:
				obfuscation_file=confdic["obfuscation_file"]
		except KeyError as missing_key:
			print "**FATAL - missing parameter %s in configuration file. check config/config-example.yaml for reference" % (missing_key, )
			sys.exit()
		
		self.load_obfuscation(obfuscation_file)
		
		
		
		
	def load_obfuscation(self, obfuscation_file):
		"""
			Reads the file with the obfuscation mappings and store the data in a python
			dictionary. The dictionary can be used in the mysql postgresql libraries to 
			run the obfuscation.
			
			:param obfuscation_file: the yaml file with the obfuscation mappings
		"""
		if not os.path.isfile(obfuscation_file):
			print "**FATAL - obfuscation file missing or invalid in %s **\ncopy change the obfuscation_file parameter in the connfiguration file." % (obfuscation_file)
			sys.exit()
		obfile=open(obfuscation_file, 'rb')
		self.obfdic=yaml.load(obfile.read())
		obfile.close()
	
	def load_snapshots(self):
		"""
			Reads the file with the snapshot definitions.
			This method  is called when taking a static snapshot from schemas not replicated.
		"""
		snpfile=open(self.snapshots_file, 'rb')
		self.snapdic=yaml.load(snpfile.read())
		snpfile.close()
		
	def get_source_name(self, config_name = 'default'):
		"""
		The method tries to set the parameter source_name determined from the configuration file.
		The value is used to query the replica catalog in order to get the source sstatus in method list_config().
		
		:param config_name: the configuration file to use. If omitted is set to default.
		"""
		
		config_file = '%s/%s.yaml' % (self.config_dir, config_name)
		self.config_name = config_name
		if os.path.isfile(config_file):
			conffile = open(config_file, 'rb')
			confdic = yaml.load(conffile.read())
			conffile.close()
			try:
				source_name=confdic["source_name"]
			except:
				print('FATAL - missing parameter source name in config file %s' % config_file)
				source_name='NOT CONFIGURED'
		return source_name
		
class replica_engine(object):
	"""
		This class wraps the interface to the replica operations and bridges mysql and postgresql engines. 
		The constructor inits the global configuration class  and setup the mysql and postgresql engines as class attributes. 
		The class initialises the logging using the configuration parameter (e.g. log level debug on stdout).
		
		:param command: the command specified on the pg_ninja.py command line. This value is used to generate the log filename (see the class global_config).
		
	"""
	def __init__(self, config, stdout=False):
		
		self.global_config=global_config(config)
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)
		self.logger.propagate = False
		self.lst_yes= ['yes',  'Yes', 'y', 'Y']
		formatter = logging.Formatter("%(asctime)s: [%(levelname)s] - %(filename)s (%(lineno)s): %(message)s", "%b %e %H:%M:%S")
		
		if self.global_config.log_dest=='stdout':
			fh=logging.StreamHandler(sys.stdout)
			
		elif self.global_config.log_dest=='file':
			fh = TimedRotatingFileHandler(self.global_config.log_file, when="d",interval=1,backupCount=self.global_config.log_days_keep)
		
		
		if self.global_config.log_level=='debug':
			fh.setLevel(logging.DEBUG)
		elif self.global_config.log_level=='info':
			fh.setLevel(logging.INFO)
			
		fh.setFormatter(formatter)
		self.logger.addHandler(fh)

		self.my_eng=mysql_engine(self.global_config, self.logger)
		self.pg_eng=pg_engine(self.global_config, self.my_eng.my_tables, self.logger)
		self.pid_file=self.global_config.pid_file
		self.exit_file="pid/exit_process.trg"
		self.email_alerts=email_alerts(self.global_config.email_config, self.logger)
	
	
	def sync_snapshot(self, snap_item, mysql_conn):
		"""
			This method syncronise a snaphost. Requires a valid snapshot name. See the snapshot-example.yaml
			for the snapshot details.
		"""
		snap_data=self.global_config.snapdic[snap_item]
		try:
			drop_tables = snap_data["drop_tables"]
		except:
			drop_tables = True
		#try:
		self.logger.info("starting snapshot for item %s" % snap_item )
		mysql_conn.connect_snapshot(snap_data)
		mysql_snap=mysql_snapshot(mysql_conn, self.global_config, self.logger)
		mysql_snap.get_snapshot_metadata(snap_data)
		pg_eng=pg_engine(self.global_config, mysql_snap.my_tables, self.logger)
		pg_eng.pg_conn.disconnect_db()
		pg_eng.pg_conn.connect_db(destination_schema=snap_data["destination_schema"])
		
		pg_eng.create_schema()
		self.logger.info("Loading snapshot for %s"  % snap_item)
		
		if drop_tables:
			pg_eng.build_tab_ddl()
			pg_eng.create_tables(True, False)
			mysql_snap.copy_table_data(pg_eng, snap_data, limit=snap_data["copy_max_size"])
			pg_eng.build_idx_ddl({})
			pg_eng.create_indices()
		else:
			pg_eng.get_index_def(snap_data["tables_limit"])
			pg_eng.drop_src_indices()
			pg_eng.truncate_tables()
			mysql_snap.copy_table_data(pg_eng, snap_data, limit=snap_data["copy_max_size"])
			pg_eng.create_src_indices()
			
		pg_eng.reset_sequences(destination_schema=snap_data["destination_schema"])
		mysql_conn.disconnect_snapshot()
	
	def take_snapshot(self, snapshot):
		"""
			method to execute the snapshots stored in the snapshot configuration file (see global_config for the details). 
			
			:param snapshot: the snapshot name. if the value is 'all' then the process will loop trough all the snapshots available.
			
		"""
		mysql_conn=mysql_connection(self.global_config)
		self.global_config.load_snapshots()
		if snapshot == 'all':
			for snap_item in self.global_config.snapdic:
				self.sync_snapshot(snap_item,  mysql_conn)
		else:
			try:
				self.sync_snapshot(snapshot,  mysql_conn)
			except:
				self.logger.debug("Snapshot %s not present in configuration file"  % snapshot)
				sys.exit()
				
				
	def wait_for_replica_end(self):
		""" 
			the method checks if the replica is running using the method check_running every 5 seconds until the replica is stopped.
		"""
		while True:
			replica_running=self.check_running(write_pid=False)
			if not replica_running:
				break
			time.sleep(5)
		
	def stop_replica(self, allow_restart=True):
		"""
			The method creates the exit file then waits for the replica end.
			
			:param allow_restart=True: if set to true the exit file is removed when the replica is stopped, allowing the process to restart. 
			if set to false the exit file is left in place leaving the replica disabled.
		"""
		exit=open(self.exit_file, 'w')
		exit.close()
		self.wait_for_replica_end()
		if allow_restart:
			os.remove(self.exit_file)
	
	def enable_replica(self):
		"""
			the method remove the exit file allowing the replica start
		"""
		try:
			os.remove(self.exit_file)
			self.logger.info("Replica enabled")
		except:
			self.logger.info("Replica already enabled")
	
		
	def sync_obfuscation(self, send_email=True):
		"""
			the function sync the obfuscated tables using the obfuscation file indicated in the configuration.
			The replica is stopped and disabled before starting the obfuscation sync.
			Check README.rst, obfuscation-example.yaml and global_config for the details.
			
			:param send_email=True: if true an email is sent when the process is complete.
		"""
		self.stop_replica(allow_restart=False)
		self.pg_eng.sync_obfuscation(self.global_config.obfdic)
		self.logger.info("Sync complete, replica can be restarted")
		if send_email:
			self.enable_replica()
			self.email_alerts.send_end_sync_obfuscation()
			
	
	def  create_schema(self, drop_tables=False):
		"""
			The method builds a new database schema on PostgreSQL using the metadata extracted from MySQL.
			
			:param drop_tables=False:  specifies whether the existing tables should be dropped before creating the schema.  
		"""
		self.pg_eng.create_schema()
		self.logger.info("Importing mysql schema")
		self.pg_eng.build_tab_ddl()
		self.pg_eng.create_tables(drop_tables)
	
	def create_views(self):
		"""
			The method creates the views exposing the tables with not obfuscated fields in the obfuscated schema.
		"""
		self.pg_eng.create_views(self.global_config.obfdic)
	
	def  create_indices(self):
		"""
			The method builds the indices on the PostgreSQL schema using the metadata extracted from MySQL.
			The DDL are built determining which tables are in the schema in clear only  and which are in both schemas in clear and obfuscated.
			
		"""
		self.pg_eng.build_idx_ddl(self.global_config.obfdic)
		self.pg_eng.create_indices()
	
	def create_service_schema(self):
		"""
			Creates the service schema sch_chameleon on the PostgreSQL database. 
			The service schema is used by the replica system for tracking the replica status and storing the 
			replicated row images.
	
		"""
		self.pg_eng.create_service_schema()
		
	def upgrade_service_schema(self):
		"""
			Migrates the service schema to the latest version if required.
			
			
		"""
		self.pg_eng.upgrade_service_schema()
		
	def drop_service_schema(self):
		"""
			Drops the service schema. This action discards any information relative to the replica.

		"""
		self.logger.info("Dropping the service schema")
		self.pg_eng.drop_service_schema()
		
	def check_running(self, write_pid=True):
		""" 	
			checks if the replica process is running. saves the pid file if not 
			:param write_pid=True: specify whether a pid file should be written if the process is missing.
		"""
		
		return_to_os=False 
		try:
			file_pid=open(self.pid_file,'rb')
			pid=file_pid.read()
			file_pid.close()
			os.kill(int(pid),0)
			return_to_os=True
		except:
			if write_pid:
				pid=os.getpid()
				file_pid=open(self.pid_file,'wb')
				file_pid.write(str(pid))
				file_pid.close()
			return_to_os=False
		
		return return_to_os
	
	def check_request_exit(self):
		"""
			checks if the exit file is present. if the exit file  is present  the method removes the pid file and returns
			true. Otherwise returns false.
		"""
		return_to_os=False
		if os.path.isfile(self.exit_file):
			print ("exit file detected, removing the pid file and terminating the replica process")
			os.remove(self.pid_file)
			print ("you shall remove the file %s before starting the replica process " % self.exit_file)
			return_to_os=True
		return return_to_os
		
	def run_replica(self):
		"""
			This is the main loop replica method.
			When executed checks if the replica is already running using the method check_running. 
			The method check_request_exit is also used for determining whether the exit file is present (replica disabled).
			If the replica is started an email is sent using the class email_alerts.
			The email configuration is set in the config.yaml file. Check the config-example.yaml for the details.
		"""
		if self.check_running() or self.check_request_exit():
			sys.exit()
		dt=datetime.now()
		str_date=dt.strftime('%Y-%m-%d %H:%M:%S')
		restart_log = self.global_config.log_dir +"restart.log"
		with open(restart_log, "ab") as restart_file:
			restart_file.write("%s - starting replica process \n" % (str_date))
			restart_file.close()
		self.email_alerts.send_start_replica_email()
		while True:
			self.my_eng.run_replica(self.pg_eng)
			self.logger.debug("batch complete. sleeping 1 second")
			time.sleep(1)
			if self.check_request_exit():
				sys.exit()
			
		
			
	def copy_table_data(self, copy_obfus=True):
		"""
			The method copies the replicated tables from mysql to postgres.
			When the copy is finished the master's coordinates are saved in the postgres service schema.
			The copy locks the tables with FLUSH TABLES WITH READ LOCK; This actually locks in read only mode the mysql database.
			After the copy is complete the table are unlocked.
		"""
		self.my_eng.copy_table_data(self.pg_eng, limit=self.global_config.copy_max_size, copy_obfuscated=copy_obfus)
		self.pg_eng.save_master_status(self.my_eng.master_status, cleanup=True)

	def init_replica(self):
		"""
			This method initialise a fresh replica.
			It stops the eventually running replica. 
			Drops and recreate a new service schema.
			Creates the clear and obfuscated schemas dropping the existing tables.
			Copies the table data.
			Create the indices and the views in the obfuscated schema.
			Enable the replica and sends an email of init_replica complete.
		"""
		self.stop_replica(allow_restart=False)
		self.drop_service_schema()
		self.create_service_schema()
		self.create_schema(drop_tables=True)
		self.copy_table_data()
		self.create_indices()
		self.create_views()
		self.enable_replica()
		self.email_alerts.send_end_init_replica()
		
	
	def sync_replica(self):
		"""
			This method is similar to the init_replica with some notable exceptions.
			The tables on postgresql are not dropped. 
			All indices on the existing tables are dropped for speeding up the reload.
			The tables are truncated. However if the truncate is not possible a delete and vacuum is executed.
			The copy table data doesn't copy the obfuscated data.
			The indices are created using the informations collected in the pg_engine's method get_index_def.
			The method sync_obfuscation is used to sync the obfuscation in a separate process.
		"""
		self.stop_replica(allow_restart=False)
		self.pg_eng.get_index_def()
		self.pg_eng.drop_src_indices()
		self.pg_eng.truncate_tables()
		self.copy_table_data(copy_obfus=False)
		self.pg_eng.create_src_indices()
		self.sync_obfuscation(False)
		self.enable_replica()
		self.email_alerts.send_end_sync_replica()
	
	def add_source(self):
		"""
			register the configuration source in the replica catalogue
		"""
		source_name=self.global_config.source_name
		schema_clear=self.global_config.schema_clear
		schema_obf=self.global_config.schema_obf
		self.pg_eng.add_source(source_name, schema_clear, schema_obf)

	def drop_source(self):
		"""
			remove the configuration source and all the replica informations associated with the source from the replica catalogue
		"""
		source_name = self.global_config.source_name
		drp_msg = 'Dropping the source %s will remove drop any replica reference.\n Are you sure? YES/No\n'  % source_name
		if sys.version_info[0] == 3:
			drop_src = input(drp_msg)
		else:
			drop_src = raw_input(drp_msg)
		if drop_src == 'YES':
			self.pg_eng.drop_source(self.global_config.source_name)
		elif drop_src in  self.lst_yes:
			print('Please type YES all uppercase to confirm')
		sys.exit()
		
	def list_config(self):
		"""
			List the available configurations stored in config/
		"""
		lst_skip = [
			'config-example',  
			'obfuscation-example',  
			'snapshots-example', 
			'obfuscation'
		]
		list_config = (os.listdir(self.global_config.config_dir))
		tab_headers = ['Config file',  'Source name',  'Status']
		tab_body = []
		
		for file in list_config:
			lst_file = file.split('.')
			file_name = lst_file[0]
			file_ext = lst_file[1]
			if file_ext == 'yaml' and file_name not in lst_skip:
				source_name = self.global_config.get_source_name(file_name)
				source_status = self.pg_eng.get_source_status(source_name)
				tab_row = [  '%s.%s' % (file_name, file_ext), source_name, source_status]
				tab_body.append(tab_row)
		print(tabulate(tab_body, headers=tab_headers))
	def show_status(self):
		"""
			list the replica status using the configuration files and the replica catalogue
		"""
		source_status=self.pg_eng.get_status()
		tab_headers = ['Config file',  'Sch. clear', 'Sch. obfuscate',  'Status' ,  'Lag',  'Last received event']
		tab_body = []
			
		for status in source_status:
			source_name = status[0]
			dest_schema = status[1]
			source_status = status[2]
			seconds_behind_master = status[3]
			last_received_event = status[4]
			obf_schema = status[5]
			tab_row = [source_name, dest_schema, obf_schema, source_status, seconds_behind_master, last_received_event ]
			tab_body.append(tab_row)
		print(tabulate(tab_body, headers=tab_headers))
