from pg_ninja import mysql_connection, mysql_engine, pg_engine,  email_alerts
import yaml
import sys
import os
import time
import logging
from tabulate import tabulate
from logging.handlers  import TimedRotatingFileHandler
from datetime import datetime
from distutils.sysconfig import get_python_lib
from shutil import copy


class config_dir(object):
	""" 
		Class used to setup the local user configuration directory.
		The class constructor sets only the class variables for the method set_config.
		The function get_python_lib() is used to determine the python library where pg_chameleon is installed.
	"""
	def __init__(self):
		"""
			Class constructor.
		"""
		python_lib=get_python_lib()
		cham_dir = "%s/.pg_ninja" % os.path.expanduser('~')	
		local_config = "%s/config/" % cham_dir 
		local_logs = "%s/logs/" % cham_dir 
		local_pid = "%s/pid/" % cham_dir 
		self.global_config_example = '%s/pg_ninja/config/config-example.yaml' % python_lib
		self.local_config_example = '%s/config-example.yaml' % local_config
		self.global_obfuscation_example = '%s/pg_ninja/config/obfuscation-example.yaml' % python_lib
		self.local_obfuscation_example = '%s/obfuscation-example.yaml' % local_config
		self.conf_dirs=[
			cham_dir, 
			local_config, 
			local_logs, 
			local_pid, 
			
		]
		
	def set_config(self):
		""" 
			The method loops the list self.conf_dirs creating it only if missing.
			
			The method checks the freshness of the config-example.yaml file and copies the new version
			from the python library determined in the class constructor with get_python_lib().
			
			If the configuration file is missing the method copies the file with a different message.
		
		"""
		for confdir in self.conf_dirs:
			if not os.path.isdir(confdir):
				print ("creating directory %s" % confdir)
				os.mkdir(confdir)
		
		if os.path.isfile(self.local_config_example):
			if os.path.getctime(self.global_config_example)>os.path.getctime(self.local_config_example):
				print ("updating config_example %s" % self.local_config_example)
				copy(self.global_config_example, self.local_config_example)
		else:
			print ("copying config_example %s" % self.local_config_example)
			copy(self.global_config_example, self.local_config_example)
			
		if os.path.isfile(self.local_obfuscation_example):
			if os.path.getctime(self.global_obfuscation_example)>os.path.getctime(self.local_obfuscation_example):
				print ("updating obfuscation_example %s" % self.local_obfuscation_example)
				copy(self.global_obfuscation_example, self.local_obfuscation_example)
		else:
			print ("copying obfuscation_example %s" % self.local_obfuscation_example)
			copy(self.global_obfuscation_example, self.local_obfuscation_example)
		
		

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
		
		obfuscation_file='config/obfuscation.yaml'
		
		python_lib=get_python_lib()
		cham_dir = "%s/.pg_ninja" % os.path.expanduser('~')	
		config_dir = '%s/config/' % cham_dir
		sql_dir = "%s/pg_ninja/sql/" % python_lib
		
		if os.path.isdir(sql_dir):
				self.sql_dir = sql_dir
		else:
			print("**FATAL - sql directory %s missing "  % sql_dir)
			sys.exit(1)
			
		config_file = '%s/%s.yaml' % (config_dir, config_name)
		if os.path.isfile(config_file):
			self.config_name = config_name
			self.config_dir = config_dir
		else:
			print("**FATAL - could not find the configuration file %s.yaml in %s"  % (config_name, config_dir))
			sys.exit(2)
			
		conffile=open(config_file, 'rb')
		confdic=yaml.load(conffile.read())
		conffile.close()
		self.lst_yes= ['yes',  'Yes', 'y', 'Y']
		
		
		
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
			self.copy_mode=confdic["copy_mode"]
			self.hexify=confdic["hexify"]
			self.log_level=confdic["log_level"]
			self.log_dest=confdic["log_dest"]
			self.log_file = os.path.expanduser(confdic["log_dir"])+config_name+'.log'
			self.pid_file = os.path.expanduser(confdic["pid_dir"])+"/"+config_name+".pid"
			self.exit_file = os.path.expanduser(confdic["pid_dir"])+"/"+config_name+".lock"
			
			self.log_dir=confdic["log_dir"]
			self.email_config=confdic["email_config"]
			self.log_days_keep = confdic["log_days_keep"]
			self.skip_view = confdic["skip_view"]
			self.out_dir = confdic["out_dir"]
			self.sleep_loop = confdic["sleep_loop"]
			self.source_name= confdic["source_name"]
			self.batch_retention = confdic["batch_retention"]
			if confdic["obfuscation_file"]:
				obfuscation_file=confdic["obfuscation_file"]
			copy_max_memory = str(confdic["copy_max_memory"])[:-1]
			copy_scale = str(confdic["copy_max_memory"])[-1]
			try:
				int(copy_scale)
				copy_max_memory = confdic["copy_max_memory"]
			except:
				if copy_scale =='k':
					copy_max_memory = str(int(copy_max_memory)*1024)
				elif copy_scale =='M':
					copy_max_memory = str(int(copy_max_memory)*1024*1024)
				elif copy_scale =='G':
					copy_max_memory = str(int(copy_max_memory)*1024*1024*1024)
				else:
					print("**FATAL - invalid suffix in parameter copy_max_memory  (accepted values are (k)ilobytes, (M)egabytes, (G)igabytes.")
					sys.exit(3)
			self.copy_max_memory = copy_max_memory
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
		if  os.path.isfile(obfuscation_file):
			obfile=open(obfuscation_file, 'rb')
			self.obfdic=yaml.load(obfile.read())
			obfile.close()
		else:
			self.obfdic = {}
		
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
	def __init__(self, config, debug_mode=False):
		
		self.debug_mode = debug_mode
		self.global_config=global_config(config)
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)
		self.logger.propagate = False
		self.lst_yes= ['yes',  'Yes', 'y', 'Y']
		formatter = logging.Formatter("%(asctime)s: [%(levelname)s] - %(filename)s (%(lineno)s): %(message)s", "%b %e %H:%M:%S")
		
		if self.global_config.log_dest=='stdout' or self.debug_mode:
			fh=logging.StreamHandler(sys.stdout)
		elif self.global_config.log_dest=='file':
			fh = TimedRotatingFileHandler(self.global_config.log_file, when="d",interval=1,backupCount=self.global_config.log_days_keep)
		
		
		if self.global_config.log_level=='debug' or self.debug_mode:
			fh.setLevel(logging.DEBUG)
		elif self.global_config.log_level=='info':
			fh.setLevel(logging.INFO)
			
		fh.setFormatter(formatter)
		self.logger.addHandler(fh)

		self.my_eng=mysql_engine(self.global_config, self.logger)
		self.pg_eng=pg_engine(self.global_config, self.my_eng.my_tables, self.logger, self.global_config.sql_dir)
		self.pid_file=self.global_config.pid_file
		self.exit_file=self.global_config.exit_file
		self.email_alerts=email_alerts(self.global_config.email_config, self.logger)
		self.sleep_loop=self.global_config.sleep_loop
	
				
				
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
	
		
	def sync_obfuscation(self, send_email=True, table='*', cleanup_idx=False):
		"""
			the function sync the obfuscated tables using the obfuscation file indicated in the configuration.
			The replica is stopped and disabled before starting the obfuscation sync.
			Check README.rst, obfuscation-example.yaml and global_config for the details.
			
			:param send_email=True: if true an email is sent when the process is complete.
		"""
		self.pg_eng.table_limit=table.split(',')
		self.stop_replica(allow_restart=False)
		self.pg_eng.set_source_id('initialising')
		self.pg_eng.sync_obfuscation(self.global_config.obfdic, cleanup_idx)
		self.pg_eng.set_source_id('initialised')
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
			Creates the service schema sch_ninja on the PostgreSQL database. 
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
		self.pg_eng.set_source_id('running')
		while True:
			self.my_eng.run_replica(self.pg_eng)
			self.logger.info("batch complete. sleeping %s second(s)" % (self.sleep_loop, ))
			time.sleep(self.sleep_loop)
			if self.check_request_exit():
				break
		self.pg_eng.set_source_id('stopped')
			
		
	def copy_table_data(self, copy_obfus=True):
		"""
			The method copies the replicated tables from mysql to postgres.
			When the copy is finished the master's coordinates are saved in the postgres service schema.
			The copy locks the tables with FLUSH TABLES WITH READ LOCK; This actually locks in read only mode the mysql database.
			After the copy is complete the table are unlocked.
		"""
		self.my_eng.copy_table_data(self.pg_eng, copy_max_memory=self.global_config.copy_max_memory, copy_obfuscated=copy_obfus)
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
		self.pg_eng.set_source_id('initialising')
		self.pg_eng.clean_batch_data()
		self.create_schema(drop_tables=True)
		self.copy_table_data()
		self.create_indices()
		if self.global_config.obfdic != {}:
			self.create_views()
		self.pg_eng.set_source_id('initialised')
		self.enable_replica()
		if not self.debug_mode:
			self.email_alerts.send_end_init_replica()
		
		
	
	def sync_tables(self, table):
		"""
			syncronise single tables with the mysql master.
			The process attempts to drop the existing tables or add them if not present.
			The tables are stored in the replica catalogue with their master's coordinates and are ignored until the replica process reaches
			the correct position. 
			:param table: comma separated list of table names to synchronise
		"""
		if table != "*":
			table_limit = table.split(',')
			self.my_eng.lock_tables()
			self.pg_eng.table_limit = table_limit
			self.pg_eng.master_status = self.my_eng.master_status
			self.pg_eng.set_source_id('initialising')
			self.stop_replica(allow_restart=False)
			self.pg_eng.build_tab_ddl()
			self.pg_eng.drop_tables()
			self.pg_eng.create_tables()
			self.my_eng.copy_table_data(self.pg_eng,  self.global_config.copy_max_memory, False, False)
			self.pg_eng.create_indices()
			self.pg_eng.set_source_id('initialised')
			self.my_eng.unlock_tables()
			self.enable_replica()
		else:
			print("You should specify at least one table to synchronise.")
	
	

			
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
			lag = status[3]
			last_received_event = status[4]
			obf_schema = status[5]
			tab_row = [source_name, dest_schema, obf_schema, source_status, lag, last_received_event ]
			tab_body.append(tab_row)
		print(tabulate(tab_body, headers=tab_headers))
