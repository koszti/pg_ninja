from pg_ninja import mysql_connection, mysql_engine, pg_engine, mysql_snapshot, email_alerts
import yaml
import sys
import os
import time
import logging
from datetime import datetime


class global_config:
	"""
		This class parses the configuration file which is in config/config.yaml and sets 
		the class variables used by the other libraries. 
		The constructor checks if the configuration file is present and if is missing emits an error message followed by
		the sys.exit() command. If the configuration file is successful parsed the class variables are set from the
		configuration values.
		The class sets the log output file from the parameter command.  If the log destination is stdout then the logfile is ignored
		
		:param command: the command specified on the pg_ninja.py command line
	
	"""
	def __init__(self,command):
		"""
			Class  constructor.
		"""
		config_file='config/config.yaml'
		obfuscation_file='config/obfuscation.yaml'
		self.snapshots_file='config/snapshots.yaml'
		if not os.path.isfile(config_file):
			print "**FATAL - configuration file missing **\ncopy config/config-example.yaml to "+config_file+" and set your connection settings."
			sys.exit()
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
			self.tables_limit=confdic["tables_limit"]
			self.exclude_tables=confdic["exclude_tables"]
			self.copy_max_size=confdic["copy_max_size"]
			self.copy_mode=confdic["copy_mode"]
			self.hexify=confdic["hexify"]
			self.log_level=confdic["log_level"]
			self.log_dest=confdic["log_dest"]
			self.copy_override=confdic["copy_override"]
			self.log_file=confdic["log_dir"]+"/"+command+'.log'
			self.pid_file=confdic["pid_dir"]+"/replica.pid"
			self.log_dir=confdic["log_dir"]
			self.email_config=confdic["email_config"]
			self.log_append = confdic["log_append"]
			self.skip_view = confdic["skip_view"]
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
			Is used for taking a static snapshot from schemas not replicated.
			
		"""
		snpfile=open(self.snapshots_file, 'rb')
		self.snapdic=yaml.load(snpfile.read())
		snpfile.close()
		

		
class replica_engine:
	"""
		This class acts as bridge between the mysql and postgresql engines. The constructor inits the global configuration
		class  and setup the mysql and postgresql engines as class objects. 
		The class setup the logging using the configuration parameter (e.g. log level debug on stdout).
		
		:param command: the command specified on the pg_ninja.py command line
		
	"""
	def __init__(self, command):
		self.global_config=global_config(command)
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)
		self.logger.propagate = False
		formatter = logging.Formatter("%(asctime)s: [%(levelname)s] - %(filename)s: %(message)s", "%b %e %H:%M:%S")
		
		if self.global_config.log_dest=='stdout':
			fh=logging.StreamHandler(sys.stdout)
			
		elif self.global_config.log_dest=='file':
			if self.global_config.log_append:
				file_mode='a'
			else:
				file_mode='w'
			fh = logging.FileHandler(self.global_config.log_file, file_mode)
		
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
		""" waiting for replica end"""
		while True:
			replica_running=self.check_running(write_pid=False)
			if not replica_running:
				break
			time.sleep(5)
		
	def stop_replica(self, allow_restart=True):
		exit=open(self.exit_file, 'w')
		exit.close()
		self.wait_for_replica_end()
		if allow_restart:
			os.remove(self.exit_file)
	
	def enable_replica(self):
		try:
			os.remove(self.exit_file)
			self.logger.info("Replica enabled")
		except:
			self.logger.info("Replica already enabled")
	
		
	def sync_obfuscation(self, send_email=True):
		"""the function syncronise the obfuscated tables using the obfuscation file """
		self.stop_replica(allow_restart=False)
		self.pg_eng.sync_obfuscation(self.global_config.obfdic)
		self.logger.info("Sync complete, replica can be restarted")
		if send_email:
			self.enable_replica()
			self.email_alerts.send_end_sync_obfuscation()
			
	
	def  create_schema(self, drop_tables=False):
		"""
			Creates the database schema on PostgreSQL using the metadata extracted from MySQL.
			
			:param drop_tables=False:  specifies whether the existing tables should be dropped before creating it.  The default setting is False.
		"""
		self.pg_eng.create_schema()
		self.logger.info("Importing mysql schema")
		self.pg_eng.build_tab_ddl()
		self.pg_eng.create_tables(drop_tables)
	
	def create_views(self):
		"""
			Creates the views pointing the tables with not obfuscated fields
		"""
		self.pg_eng.create_views(self.global_config.obfdic)
	
	def  create_indices(self):
		"""
			Creates the indices on the PostgreSQL schema using the metadata extracted from MySQL.
			
		"""
		self.pg_eng.build_idx_ddl(self.global_config.obfdic)
		self.pg_eng.create_indices()
	
	def create_service_schema(self):
		"""
			Creates the service schema sch_chameleon on the PostgreSQL database. The service schema is required for having the replica working correctly.
	
		"""
		self.pg_eng.create_service_schema()
		
	def upgrade_service_schema(self):
		"""
			Upgrade the service schema to the latest version.
			
			:todo: everything!
		"""
		self.pg_eng.upgrade_service_schema()
		
	def drop_service_schema(self):
		"""
			Drops the service schema. The action discards any information relative to the replica.

		"""
		self.logger.info("Dropping the service schema")
		self.pg_eng.drop_service_schema()
		
	def check_running(self, write_pid=True):
		""" checks if the process is running. saves the pid file if not """
		
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
		return_to_os=False
		"""checks for the exit file and terminate the replica if the file is present """
		if os.path.isfile(self.exit_file):
			print "exit file detected, removing the pid file and terminating the replica process"
			os.remove(self.pid_file)
			print "you shall remove the file %s before starting the replica process " % self.exit_file
			return_to_os=True
		return return_to_os
		
	def run_replica(self):
		"""
			Runs the replica loop. 
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
			Copy the data for the replicated tables from mysql to postgres.
			
			After the copy the master's coordinates are saved in postgres.
		"""
		self.my_eng.copy_table_data(self.pg_eng, limit=self.global_config.copy_max_size, copy_obfuscated=copy_obfus)
		self.pg_eng.save_master_status(self.my_eng.master_status, cleanup=True)

	def init_replica(self):
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
		self.stop_replica(allow_restart=False)
		self.pg_eng.get_index_def()
		self.pg_eng.drop_src_indices()
		self.pg_eng.truncate_tables()
		self.copy_table_data(copy_obfus=False)
		self.pg_eng.create_src_indices()
		self.sync_obfuscation(False)
		self.enable_replica()
		self.email_alerts.send_end_sync_replica()
